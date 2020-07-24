package finder

import (
	"fmt"
	"log"
	"strconv"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/fromanirh/numalign/pkg/topologyinfo/cpus"
	"github.com/fromanirh/numalign/pkg/topologyinfo/pcidev"
	v1alpha1 "github.com/swatisehgal/topologyapi/pkg/apis/topology/v1alpha1"
)

type ResourceAggregator interface {
	Aggregate(podResData []PodResources) []v1alpha1.NUMANodeResource
}

type resourceAggregator struct {
	pciDevs         *pcidev.PCIDevices
	cpus            *cpus.CPUs
	cpuID2NUMAID    map[int]int
	pciAddr2NUMAID  map[string]int
	perNUMACapacity map[int]map[v1.ResourceName]int64
	// pciaddr -> resourcename
	pci2ResourceMap map[string]string
}

func (ra *resourceAggregator) GetPCI2ResourceMap() map[string]string {
	return ra.pci2ResourceMap
}

func NewResourceAggregator(sysfsRoot string, pciResMapConf map[string]string) (*resourceAggregator, error) {
	var err error
	resAcc := resourceAggregator{
		perNUMACapacity: make(map[int]map[v1.ResourceName]int64),
	}

	// first scan the sysfs
	// CAUTION: these resources are expected to change rarely - if ever. So we are intentionally do this once during the process lifecycle.
	resAcc.cpus, err = cpus.NewCPUs(sysfsRoot)
	if err != nil {
		return nil, fmt.Errorf("error scanning the system CPUs: %v", err)
	}
	for nodeNum, cpuList := range resAcc.cpus.NUMANodeCPUs {
		log.Printf("detected system CPU: NUMA cell %d cpus = %v\n", nodeNum, cpuList)
	}

	resAcc.pciDevs, err = pcidev.NewPCIDevices(sysfsRoot)
	if err != nil {
		return nil, fmt.Errorf("error scanning the system PCI devices: %v", err)
	}
	for _, pciDev := range resAcc.pciDevs.Items {
		log.Printf("detected system PCI device = %s\n", pciDev.String())
	}

	// helper maps
	var pciDevMap map[int]map[v1.ResourceName]int64
	pciDevMap, resAcc.pci2ResourceMap, resAcc.pciAddr2NUMAID = makePCI2ResourceMap(resAcc.cpus.NUMANodes, resAcc.pciDevs, pciResMapConf)
	resAcc.cpuID2NUMAID = make(map[int]int)
	for nodeNum, cpus := range resAcc.cpus.NUMANodeCPUs {
		for _, cpu := range cpus {
			resAcc.cpuID2NUMAID[cpu] = nodeNum
		}
	}

	// initialize with the capacities
	for nodeNum := 0; nodeNum < resAcc.cpus.NUMANodes; nodeNum++ {
		resAcc.perNUMACapacity[nodeNum] = make(map[v1.ResourceName]int64)
		for resName, count := range pciDevMap[nodeNum] {
			resAcc.perNUMACapacity[nodeNum][resName] = count
		}

		cpus := resAcc.cpus.NUMANodeCPUs[nodeNum]
		resAcc.perNUMACapacity[nodeNum][v1.ResourceCPU] = int64(len(cpus))
	}

	return &resAcc, nil
}

func (ra *resourceAggregator) updateNUMAMap(numaData map[int]map[v1.ResourceName]int64, ri ResourceInfo) {
	if ri.Name == v1.ResourceCPU {
		for _, cpuIDStr := range ri.Data {
			cpuID, err := strconv.Atoi(cpuIDStr)
			if err != nil {
				// TODO: log
				continue
			}
			nodeNum, ok := ra.cpuID2NUMAID[cpuID]
			if !ok {
				// TODO: log
				continue
			}
			numaData[nodeNum][ri.Name]--
		}
		return
	}
	for _, pciAddr := range ri.Data {
		nodeNum, ok := ra.pciAddr2NUMAID[pciAddr]
		if !ok {
			// TODO: log
			continue
		}
		numaData[nodeNum][ri.Name]--
	}
}

func (ra *resourceAggregator) Aggregate(podResData []PodResources) []v1alpha1.NUMANodeResource {
	var perNumaRes []v1alpha1.NUMANodeResource

	perNuma := make(map[int]map[v1.ResourceName]int64)
	for nodeNum, nodeRes := range ra.perNUMACapacity {
		perNuma[nodeNum] = make(map[v1.ResourceName]int64)
		for resName, resCap := range nodeRes {
			perNuma[nodeNum][resName] = resCap
		}
	}

	for _, podRes := range podResData {
		for _, contRes := range podRes.Containers {
			for _, res := range contRes.Resources {
				ra.updateNUMAMap(perNuma, res)
			}
		}
	}

	for nodeNum, resList := range perNuma {
		numaRes := v1alpha1.NUMANodeResource{
			NUMAID:    nodeNum,
			Resources: make(v1.ResourceList),
		}
		for name, intQty := range resList {
			numaRes.Resources[name] = *resource.NewQuantity(intQty, resource.DecimalSI)
		}
		perNumaRes = append(perNumaRes, numaRes)
	}
	return perNumaRes
}

func makePCI2ResourceMap(numaNodes int, pciDevs *pcidev.PCIDevices, pciResMapConf map[string]string) (map[int]map[v1.ResourceName]int64, map[string]string, map[string]int) {
	pciAddr2NUMAID := make(map[string]int)
	pci2Res := make(map[string]string)

	perNUMACapacity := make(map[int]map[v1.ResourceName]int64)
	for nodeNum := 0; nodeNum < numaNodes; nodeNum++ {
		perNUMACapacity[nodeNum] = make(map[v1.ResourceName]int64)

		for _, pciDev := range pciDevs.Items {
			if pciDev.NUMANode() != nodeNum {
				continue
			}
			sriovDev, ok := pciDev.(pcidev.SRIOVDeviceInfo)
			if !ok {
				continue
			}

			if !sriovDev.IsVFn {
				continue
			}

			resName, ok := pciResMapConf[sriovDev.ParentFn]
			if !ok {
				continue
			}

			pci2Res[sriovDev.Address()] = resName
			pciAddr2NUMAID[sriovDev.Address()] = nodeNum
			perNUMACapacity[nodeNum][v1.ResourceName(resName)]++
		}
	}
	return perNUMACapacity, pci2Res, pciAddr2NUMAID
}
