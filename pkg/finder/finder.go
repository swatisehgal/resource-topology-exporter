package finder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	runtimespec "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"

	"k8s.io/api/core/v1"
	pb "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/util"
)

const (
	defaultTimeout     = 5 * time.Second
	CGroupCPUSetPrefix = "fs/cgroup/cpuset"
	CGroupCPUsetSuffix = "cpuset.cpus"
)

type Args struct {
	ContainerRuntime  string
	CRIEndpointPath   string
	SleepInterval     time.Duration
	Namespace         string
	SysfsRoot         string
	SRIOVConfigFile   string
	KubeletConfigFile string
}

type CRIFinder interface {
	Scan() ([]PodResources, error)
}

type CGroupPathTranslator func(sysfs, cgroupPath string) string

type criFinder struct {
	// we may want to move to cadvisor past PoC stage
	args            Args
	conn            *grpc.ClientConn
	client          pb.RuntimeServiceClient
	cgroupTranslate CGroupPathTranslator
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

func NewFinder(args Args, pci2ResourceMap map[string]string) (CRIFinder, error) {
	finderInstance := &criFinder{
		args:            args,
		pci2ResourceMap: pci2ResourceMap,
	}

	// At this stage, we only support containerd and cri-o
	if args.ContainerRuntime == "containerd" {
		finderInstance.cgroupTranslate = containerDCGroupPathTranslate
	} else {
		//cri-o
		finderInstance.cgroupTranslate = crioCGroupPathTranslate
	}

	// now we can connext to CRI
	addr, dialer, err := getAddressAndDialer(finderInstance.args.CRIEndpointPath)
	if err != nil {
		return nil, err
	}

	finderInstance.conn, err = grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(defaultTimeout), grpc.WithContextDialer(dialer))
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

func getAddressAndDialer(endpoint string) (string, func(ctx context.Context, addr string) (net.Conn, error), error) {
	return util.GetAddressAndDialer(endpoint)
}

func (cpf *criFinder) listContainersResponse() (*pb.ListContainersResponse, error) {
	st := &pb.ContainerStateValue{}
	st.State = pb.ContainerState_CONTAINER_RUNNING
	filter := &pb.ContainerFilter{}
	filter.State = st

	ListContReq := &pb.ListContainersRequest{
		Filter: filter,
	}

	ListContResponse, err := cpf.client.ListContainers(context.Background(), ListContReq)
	if err != nil {
		fmt.Errorf("Error in  ListContResponse: %v", err)
		return nil, err
	}
	return ListContResponse, nil
}

func (cpf *criFinder) containerStatsResponse(c *pb.Container) (*pb.ContainerStatsResponse, error) {
	//ContainerStats
	ContStatsReq := &pb.ContainerStatsRequest{
		ContainerId: c.Id,
	}
	ContStatsResp, err := cpf.client.ContainerStats(context.Background(), ContStatsReq)
	if err != nil {
		log.Printf("Error in  ContStatsResp: %v", err)
		return nil, err
	}
	return ContStatsResp, nil
}

func (cpf *criFinder) containerStatusResponse(c *pb.Container) (*pb.ContainerStatusResponse, error) {
	//ContainerStatus
	ContStatusReq := &pb.ContainerStatusRequest{
		ContainerId: c.Id,
		Verbose:     true,
	}
	ContStatusResp, err := cpf.client.ContainerStatus(context.Background(), ContStatusReq)
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

func (cpf *criFinder) isWatchable(podSb *pb.PodSandbox) bool {
	if cpf.args.Namespace == "" {
		return true
	}
	//TODO:  add an explicit check for guaranteed pods
	return cpf.args.Namespace == podSb.Metadata.Namespace
}

func (cpf *criFinder) Scan() ([]PodResources, error) {
	//PodSandboxStatus
	podSbResponse, err := cpf.listPodSandBoxResponse()
	if err != nil {
		return nil, err
	}
	var podResData []PodResources
	for _, podSb := range podSbResponse.GetItems() {
		if !cpf.isWatchable(podSb) {
			log.Printf("SKIP pod %q\n", podSb.Metadata.Name)
			continue
		}

		log.Printf("querying pod %q\n", podSb.Metadata.Name)
		ListContResponse, err := cpf.listContainersResponse()
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

			ContStatusResp, err := cpf.containerStatusResponse(c)
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
			if linuxResources == nil {
				log.Printf("pod %q container %q: missing linux resource infos", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name)
				continue
			}

			env := getContainerEnvironmentVariables(ci)
			if env == nil {
				log.Printf("pod %q container %q: missing environment infos", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name)
				continue
			}

			cpus, err := getAllocatedCPUs(cpf.cgroupTranslate(cpf.args.SysfsRoot, ci.RuntimeSpec.Linux.CgroupsPath))
			if err != nil {
				log.Printf("pod %q container %q unable to get allocatedCPUs %v as", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, err)
				continue
			}
			cpuList, err := cpuset.Parse(cpus)
			if err != nil {
				log.Printf("pod %q container %q unable to parse %v as CPUSet: %v", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, cpus, err)
				continue
			}
			contRes.Resources = append(contRes.Resources, makeCPUResource(cpuList)...)
			if ci.Config != nil && ci.Config.Envs != nil {
				contRes.Resources = append(contRes.Resources, makePCIDeviceResource(env, cpf.pci2ResourceMap)...)
			}

			log.Printf("pod %q container %q contData=%s\n", podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, spew.Sdump(contRes))
			podRes.Containers = append(podRes.Containers, contRes)
		}

		podResData = append(podResData, podRes)
	}
	return podResData, nil
}

func makeCPUResource(cpus cpuset.CPUSet) []ResourceInfo {
	var ret []string
	for _, cpuID := range cpus.ToSlice() {
		ret = append(ret, fmt.Sprintf("%d", cpuID))
	}
	return []ResourceInfo{
		{
			Name: v1.ResourceCPU,
			Data: ret,
		},
	}
}

func makePCIDeviceResource(env map[string]string, pci2ResMap map[string]string) []ResourceInfo {
	var resInfos []ResourceInfo
	for key, value := range env {
		if !strings.HasPrefix(key, "PCIDEVICE_") {
			continue
		}

		pciAddrs := strings.Split(value, ",")
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

func getContainerEnvironmentVariables(ci ContainerInfo) map[string]string {
	envVars := make(map[string]string)

	if ci.RuntimeSpec != nil && ci.RuntimeSpec.Process != nil && ci.RuntimeSpec.Process.Env != nil {
		for _, entry := range ci.RuntimeSpec.Process.Env {
			items := strings.SplitN(entry, "=", 2)
			if len(items) == 2 {
				envVars[items[0]] = items[1]
			}
		}
		return envVars
	}

	if ci.Config != nil && ci.Config.Envs != nil {
		for _, env := range ci.Config.Envs {
			envVars[env.Key] = env.Value
		}
		return envVars
	}

	// nothing else to try, give up and fail!
	return nil
}

func getAllocatedCPUs(cgroupAbsolutePath string) (string, error) {
	cpuSet, err := ioutil.ReadFile(cgroupAbsolutePath)
	if err != nil {
		fmt.Errorf("Can't get assigned CPUs from the Cgroup Path: %s : %v", cgroupAbsolutePath, err)
		return "", err
	}
	cpuSet = bytes.TrimSpace(cpuSet)
	return string(cpuSet), nil
}

func crioCGroupPathTranslate(sysfs, cgroupPath string) string {
	fixedCgroupPath := strings.Replace(cgroupPath, "slice:crio:", "slice/crio-", 1)
	return filepath.Join(sysfs, CGroupCPUSetPrefix, "kubepods.slice", fmt.Sprint(fixedCgroupPath, ".scope"), CGroupCPUsetSuffix)
}

func containerDCGroupPathTranslate(sysfs, cgroupPath string) string {
	return filepath.Join(sysfs, CGroupCPUSetPrefix, cgroupPath, CGroupCPUsetSuffix)
}
