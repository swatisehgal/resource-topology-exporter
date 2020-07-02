package finder

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"

	runtimespec "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	pb "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/util"

	"github.com/fromanirh/numalign/pkg/topologyinfo/cpus"
	"github.com/fromanirh/numalign/pkg/topologyinfo/pcidev"
	v1alpha1 "github.com/swatisehgal/resource-topology-exporter/pkg/apis/topocontroller/v1alpha1"
)

const (
	defaultTimeout = 5 * time.Second
)

type Args struct {
	CRIEndpointPath      string
	SleepInterval        time.Duration
	Namespace            string
	SysfsRoot            string
	PCIToResourceMapPath string
}

type CRIFinder interface {
	Scan() ([]PodResources, error)
	Aggregate(podResData []PodResources) []v1alpha1.NUMANodeResource
}

type criFinder struct {
	args   Args
	conn   *grpc.ClientConn
	client pb.RuntimeServiceClient
	// we may want to move to cadvisor past PoC stage
	pciDevs         *pcidev.PCIDevices
	cpus            *cpus.CPUs
	cpuID2NUMAID    map[int]int
	pciAddr2NUMAID  map[string]int
	perNUMACapacity map[int]map[v1.ResourceName]int64
	// pciaddr -> resourcename
	pci2ResourceMap map[string]string
}

type ContainerInfo struct {
	sandboxID      string              `json:"sandboxID"`
	Pid            uint32              `json:"pid"`
	Removing       bool                `json:"removing"`
	SnapshotKey    string              `json:"snapshotKey"`
	Snapshotter    string              `json:"snapshotter"`
	RuntimeType    string              `json:"runtimeType"`
	RuntimeOptions interface{}         `json:"runtimeOptions"`
	Config         *pb.ContainerConfig `json:"config"`
	RuntimeSpec    *runtimespec.Spec   `json:"runtimeSpec"`
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

func NewFinder(args Args, pciResMapConf map[string]string) (CRIFinder, error) {
	finderInstance := &criFinder{
		args:            args,
		perNUMACapacity: make(map[int]map[v1.ResourceName]int64),
	}
	var err error

	// first scan the sysfs
	// CAUTION: these resources are expected to change rarely - if ever. So we are intentionally do this once during the process lifecycle.
	finderInstance.cpus, err = cpus.NewCPUs(finderInstance.args.SysfsRoot)
	if err != nil {
		return nil, fmt.Errorf("error scanning the system CPUs: %v", err)
	}
	for nodeNum, cpuList := range finderInstance.cpus.NUMANodeCPUs {
		log.Printf("detected system CPU: NUMA cell %d cpus = %v\n", nodeNum, cpuList)
	}

	for nodeNum := 0; nodeNum < finderInstance.cpus.NUMANodes; nodeNum++ {
	}

	finderInstance.pciDevs, err = pcidev.NewPCIDevices(finderInstance.args.SysfsRoot)
	if err != nil {
		return nil, fmt.Errorf("error scanning the system PCI devices: %v", err)
	}
	for _, pciDev := range finderInstance.pciDevs.Items {
		log.Printf("detected system PCI device = %s\n", pciDev.String())
	}

	// helper maps
	var pciDevMap map[int]map[v1.ResourceName]int64
	pciDevMap, finderInstance.pci2ResourceMap, finderInstance.pciAddr2NUMAID = makePCI2ResourceMap(finderInstance.cpus.NUMANodes, finderInstance.pciDevs, pciResMapConf)
	finderInstance.cpuID2NUMAID = make(map[int]int)
	for nodeNum, cpus := range finderInstance.cpus.NUMANodeCPUs {
		for _, cpu := range cpus {
			finderInstance.cpuID2NUMAID[cpu] = nodeNum
		}
	}

	// initialize with the capacities
	for nodeNum := 0; nodeNum < finderInstance.cpus.NUMANodes; nodeNum++ {
		finderInstance.perNUMACapacity[nodeNum] = make(map[v1.ResourceName]int64)
		for resName, count := range pciDevMap[nodeNum] {
			finderInstance.perNUMACapacity[nodeNum][resName] = count
		}

		cpus := finderInstance.cpus.NUMANodeCPUs[nodeNum]
		finderInstance.perNUMACapacity[nodeNum][v1.ResourceCPU] = int64(len(cpus))
	}

	// now we can connext to CRI
	addr, dialer, err := getAddressAndDialer(finderInstance.args.CRIEndpointPath)
	if err != nil {
		return nil, err
	}

	finderInstance.conn, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(defaultTimeout), grpc.WithDialer(dialer))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	finderInstance.client = pb.NewRuntimeServiceClient(finderInstance.conn)
	log.Printf("connected to '%v'!", finderInstance.args.CRIEndpointPath)
	if finderInstance.args.Namespace != "" {
		log.Printf("watching namespace %q", finderInstance.args.Namespace)
	} else {
		log.Printf("watching all namespaces")
	}

	return finderInstance, nil
}

func getAddressAndDialer(endpoint string) (string, func(addr string, timeout time.Duration) (net.Conn, error), error) {
	return util.GetAddressAndDialer(endpoint)
}

func (f *criFinder) listContainersResponse() (*pb.ListContainersResponse, error) {
	st := &pb.ContainerStateValue{}
	st.State = pb.ContainerState_CONTAINER_RUNNING
	filter := &pb.ContainerFilter{}
	filter.State = st

	ListContReq := &pb.ListContainersRequest{
		Filter: filter,
	}

	ListContResponse, err := f.client.ListContainers(context.Background(), ListContReq)
	if err != nil {
		fmt.Errorf("Error in  ListContResponse: %v", err)
		return nil, err
	}
	return ListContResponse, nil
}

func (f *criFinder) containerStatsResponse(c *pb.Container) (*pb.ContainerStatsResponse, error) {
	//ContainerStats
	ContStatsReq := &pb.ContainerStatsRequest{
		ContainerId: c.Id,
	}
	ContStatsResp, err := f.client.ContainerStats(context.Background(), ContStatsReq)
	if err != nil {
		log.Printf("Error in  ContStatsResp: %v", err)
		return nil, err
	}
	return ContStatsResp, nil
}

func (f *criFinder) containerStatusResponse(c *pb.Container) (*pb.ContainerStatusResponse, error) {
	//ContainerStatus
	ContStatusReq := &pb.ContainerStatusRequest{
		ContainerId: c.Id,
		Verbose:     true,
	}
	ContStatusResp, err := f.client.ContainerStatus(context.Background(), ContStatusReq)
	if err != nil {
		log.Printf("Error in  ContStatusResp: %v", err)
		return nil, err
	}
	return ContStatusResp, nil
}

type ResourceInfo struct {
	Name v1.ResourceName
	Data []string
}

type ContainerResources struct {
	Name      string
	Resources []ResourceInfo
}

type PodResources struct {
	Name       string
	Namespace  string
	Containers []ContainerResources
}

func (cpf *criFinder) updateNUMAMap(numaData map[int]map[v1.ResourceName]int64, ri ResourceInfo) {
	if ri.Name == v1.ResourceCPU {
		for _, cpuIDStr := range ri.Data {
			cpuID, err := strconv.Atoi(cpuIDStr)
			if err != nil {
				// TODO: log
				continue
			}
			nodeNum, ok := cpf.cpuID2NUMAID[cpuID]
			if !ok {
				// TODO: log
				continue
			}
			numaData[nodeNum][ri.Name]--
		}
		return
	}
	for _, pciAddr := range ri.Data {
		nodeNum, ok := cpf.pciAddr2NUMAID[pciAddr]
		if !ok {
			// TODO: log
			continue
		}
		numaData[nodeNum][ri.Name]--
	}
}

func (cpf *criFinder) listPodSandBoxResponse() (*pb.ListPodSandboxResponse, error) {
	//ListPodSandbox
	podState := &pb.PodSandboxStateValue{}
	podState.State = pb.PodSandboxState_SANDBOX_READY
	filter := &pb.PodSandboxFilter{}
	filter.State = podState
	request := &pb.ListPodSandboxRequest{
		Filter: filter,
	}
	PodSbResponse, err := cpf.client.ListPodSandbox(context.Background(), request)
	if err != nil {
		fmt.Errorf("Error in listing ListPodSandbox : %v", err)
		return nil, err
	}
	return PodSbResponse, nil
}

func (f *criFinder) isWatchable(podSb *pb.PodSandbox) bool {
	if f.args.Namespace == "" {
		return true
	}
	//TODO:  add an explicit check for guaranteed pods
	return f.args.Namespace == podSb.Metadata.Namespace
}

func (f *criFinder) Scan() ([]PodResources, error) {
	//PodSandboxStatus
	podSbResponse, err := f.listPodSandBoxResponse()
	if err != nil {
		return nil, err
	}
	var podResData []PodResources
	for _, podSb := range podSbResponse.GetItems() {
		if !f.isWatchable(podSb) {
			log.Printf("SKIP pod %q\n", podSb.Metadata.Name)
			continue
		}

		log.Printf("querying pod %q\n", podSb.Metadata.Name)
		ListContResponse, err := f.listContainersResponse()
		if err != nil {
			log.Printf("fail to list containers for pod %q: err: %v", podSb.Metadata.Name, err)
			continue
		}

		podRes := PodResources{
			Name:      podSb.Metadata.Name,
			Namespace: podSb.Metadata.Namespace,
		}
		for _, c := range ListContResponse.GetContainers() {
			if c.PodSandboxId != podSb.Id {
				continue
			}

			log.Printf("querying pod %q container %q\n", podSb.Metadata.Name, c.Metadata.Name)

			ContStatusResp, err := f.containerStatusResponse(c)
			if err != nil {
				return nil, err
			}
			contRes := ContainerResources{
				Name: ContStatusResp.Status.Metadata.Name,
			}
			log.Printf("got status for pod %q container %q\n", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name)

			var ci ContainerInfo
			err = json.Unmarshal([]byte(ContStatusResp.Info["info"]), &ci)
			if err != nil {
				log.Printf("pod %q container %q: cannot parse status info: %v", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, err)
				continue
			}

			var linuxResources *runtimespec.LinuxResources
			if ci.RuntimeSpec.Linux != nil && ci.RuntimeSpec.Linux.Resources != nil {
				linuxResources = ci.RuntimeSpec.Linux.Resources
			}

			cpuList, err := cpuset.Parse(linuxResources.CPU.Cpus)
			if err != nil {
				log.Printf("pod %q container %q unable to parse %v as CPUSet: %v", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, linuxResources.CPU.Cpus, err)
				continue
			}
			contRes.Resources = append(contRes.Resources, makeCPUResource(cpuList)...)
			contRes.Resources = append(contRes.Resources, makePCIDeviceResource(ci.Config.Envs, f.pci2ResourceMap)...)

			log.Printf("pod %q container %q contData=%s\n", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, spew.Sdump(contRes))
			podRes.Containers = append(podRes.Containers, contRes)
		}

		podResData = append(podResData, podRes)
	}
	return podResData, nil
}

func (cpf *criFinder) Aggregate(podResData []PodResources) []v1alpha1.NUMANodeResource {
	var perNumaRes []v1alpha1.NUMANodeResource

	perNuma := make(map[int]map[v1.ResourceName]int64)
	for nodeNum, nodeRes := range cpf.perNUMACapacity {
		perNuma[nodeNum] = make(map[v1.ResourceName]int64)
		for resName, resCap := range nodeRes {
			perNuma[nodeNum][resName] = resCap
		}
	}

	for _, podRes := range podResData {
		for _, contRes := range podRes.Containers {
			for _, res := range contRes.Resources {
				cpf.updateNUMAMap(perNuma, res)
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

func makeCPUResource(cpus cpuset.CPUSet) []ResourceInfo {
	var ret []string
	for _, cpuID := range cpus.ToSlice() {
		ret = append(ret, fmt.Sprintf("%d", cpuID))
	}
	return []ResourceInfo{
		ResourceInfo{
			Name: v1.ResourceCPU,
			Data: ret,
		},
	}
}

func makePCIDeviceResource(envs []*pb.KeyValue, pci2ResMap map[string]string) []ResourceInfo {
	var resInfos []ResourceInfo
	for _, env := range envs {
		if !strings.HasPrefix(env.Key, "PCIDEVICE_") {
			continue
		}

		pciAddrs := strings.Split(env.Value, ",")
		// the assumption here that all the address per variable are bound to the same resource name

		resName, ok := pci2ResMap[pciAddrs[0]]
		if !ok {
			continue
		}

		resInfos = append(resInfos, ResourceInfo{
			Name: v1.ResourceName(resName),
			Data: pciAddrs,
		})

	}
	return resInfos
}
