package finder

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	runtimespec "github.com/opencontainers/runtime-spec/specs-go"
	v1alpha1 "github.com/swatisehgal/resource-topology-exporter/pkg/apis/topocontroller/v1alpha1"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	pb "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	podresourcesapi "k8s.io/kubernetes/pkg/kubelet/apis/podresources/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/util"
)

const (
	defaultTimeout             = 5 * time.Second
	ns                         = "resource-topology-exporter"
	defaultPodResourcesTimeout = 10 * time.Second
	defaultPodResourcesMaxSize = 1024 * 1024 * 16 // 16 Mb
)

type Args struct {
	CRIEndpointPath            string
	PodResourceAPIEndpointPath string
	SleepInterval              time.Duration
}

type Finder interface {
	Run() error
	GetPodsData() []*PodResourceData
	GetAllocatedCPUs() []v1alpha1.NUMANodeResource
	GetAllocatedDevices() []v1alpha1.NUMANodeResource
}

type finder struct {
	args              Args
	conn              *grpc.ClientConn
	client            pb.RuntimeServiceClient
	podResourceClient podresourcesapi.PodResourcesListerClient
	podsData          []*PodResourceData
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

func (f *finder) Run() error {
	err := f.UpdateCRIInfo()
	if err != nil {
		return fmt.Errorf("Unable to update CRIInfo: %v", err)

	}

	return nil
}

func (f *finder) GetPodsData() []*PodResourceData {
	return f.podsData
}

func (f *finder) GetAllocatedCPUs() []v1alpha1.NUMANodeResource {
	allocatedCpusNumaInfo := map[string][]string{}
	for _, podData := range f.GetPodsData() {
		podcpusNumaInfo := podData.GetAllocatedCPUs()
		for k, cpuList := range podcpusNumaInfo {
			for _, cpu := range cpuList {
				allocatedCpusNumaInfo[k] = append(allocatedCpusNumaInfo[k], cpu)
			}
		}
	}
	cpuResourceList := getCPUResourceList(allocatedCpusNumaInfo)
	return cpuResourceList
}

func (f *finder) GetAllocatedDevices() []v1alpha1.NUMANodeResource {
	allocatedDevsNumaInfo := map[string]map[devicePluginResourceName]int{}
	for _, podData := range f.GetPodsData() {
		podDevsNumaInfo := podData.GetAllocatedDevices()

		for numaId, devs := range podDevsNumaInfo {
			for res, n := range devs {
				if allocatedDevsNumaInfo[numaId] == nil {
					count := map[devicePluginResourceName]int{res: 0}
					allocatedDevsNumaInfo[numaId] = count
				}
				allocatedDevsNumaInfo[numaId][res] += n
			}
		}
	}
	deviceResourceList := getDeviceResourceList(allocatedDevsNumaInfo)
	return deviceResourceList
}

func getDeviceResourceList(allocatedDevsNumaInfo map[string]map[devicePluginResourceName]int) []v1alpha1.NUMANodeResource {
	var deviceNumaResources []v1alpha1.NUMANodeResource = make([]v1alpha1.NUMANodeResource, 0)
	for numaId, devs := range allocatedDevsNumaInfo {
		res := v1.ResourceList{}
		numaInt, _ := strconv.Atoi(numaId)
		for resourceName, n := range devs {
			res[v1.ResourceName(resourceName)] = *resource.NewQuantity(int64(n), resource.DecimalSI)
		}
		deviceNumaResources = append(deviceNumaResources, v1alpha1.NUMANodeResource{NUMAID: numaInt, Resources: res})
	}
	return deviceNumaResources
}

func getCPUResourceList(allocatedCpusNumaInfo map[string][]string) []v1alpha1.NUMANodeResource {
	var cpuNumaResources []v1alpha1.NUMANodeResource = make([]v1alpha1.NUMANodeResource, 0)
	for k, v := range allocatedCpusNumaInfo {
		numaId, _ := strconv.Atoi(k)
		res := v1.ResourceList{v1.ResourceName("cpu"): *resource.NewQuantity(int64(len(v)), resource.DecimalSI)}
		cpuNumaResources = append(cpuNumaResources, v1alpha1.NUMANodeResource{NUMAID: numaId, Resources: res})
	}
	return cpuNumaResources
}
func NewFinder(args Args) (Finder, error) {
	finderInstance := &finder{args: args}
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

	//Get a client for the PodResourcesLister grpc service
	addr, dialer, err = getAddressAndDialer(finderInstance.args.PodResourceAPIEndpointPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultPodResourcesTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaultPodResourcesMaxSize)))
	if err != nil {
		return nil, fmt.Errorf("error dialing socket %s: %v", finderInstance.args.PodResourceAPIEndpointPath, err)
	}

	finderInstance.podResourceClient = podresourcesapi.NewPodResourcesListerClient(conn)

	log.Printf("connected to '%v'!", finderInstance.args.PodResourceAPIEndpointPath)

	return finderInstance, nil
}

func getAddressAndDialer(endpoint string) (string, func(addr string, timeout time.Duration) (net.Conn, error), error) {
	return util.GetAddressAndDialer(endpoint)
}

func (f *finder) UpdateCRIInfo() error {
	var err error
	log.Printf("Inside Update CRI Info")
	err = f.updateInfo()
	if err != nil {
		return err
	}
	return nil
}

func (f *finder) listContainersResponse() (*pb.ListContainersResponse, error) {
	log.Printf("ListContainers CRI call\n")

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

func (f *finder) containerStatsResponse(c *pb.Container) (*pb.ContainerStatsResponse, error) {
	//ContainerStats
	log.Printf("ContainerStats CRI call\n")
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

func (f *finder) containerStatusResponse(c *pb.Container) (*pb.ContainerStatusResponse, error) {
	//ContainerStatus
	log.Printf("ContainerStatus CRI call\n")
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

func (cpf *finder) listPodSandBoxResponse() (*pb.ListPodSandboxResponse, error) {
	//ListPodSandbox
	log.Printf(" ListPodSandbox CRI call\n")
	podState := &pb.PodSandboxStateValue{}
	log.Printf("PodSandboxStateValue: %v", podState)
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

func (f *finder) updateInfo() error {
	//PodSandboxStatus
	log.Printf("PodSandboxStatus CRI call\n")
	podSbResponse, err := f.listPodSandBoxResponse()
	if err != nil {
		return err
	}
	var podResData []*PodResourceData = make([]*PodResourceData, 0)
	for _, podSb := range podSbResponse.GetItems() {
		// Assumption here is that all the pods in the default namespace are being considered (assuming they are guranteed)
		//TODO:  add an explicit check for guaranteed pods
		if podSb.Metadata.Namespace != ns {
			continue
		}
		contsData := []ContainerData{}
		ListContResponse, err := f.listContainersResponse()
		if err != nil {
			return err
		}
		for _, c := range ListContResponse.GetContainers() {
			if c.PodSandboxId != podSb.Id {
				continue
			}
			ContStatusResp, err := f.containerStatusResponse(c)
			if err != nil {
				return err
			}
			log.Printf("container name is %v\n", ContStatusResp.Status.Metadata.Name)
			var ci ContainerInfo
			b := []byte(ContStatusResp.Info["info"])
			err = json.Unmarshal(b, &ci)
			l, _ := json.Marshal(ci.RuntimeSpec.Linux)
			var linux runtimespec.Linux
			err = json.Unmarshal(l, &linux)
			res, _ := json.Marshal(linux.Resources)
			var linuxResources runtimespec.LinuxResources
			err = json.Unmarshal(res, &linuxResources)

			//device stuff here

			d, _ := json.Marshal(ci.Config.Devices)

			log.Printf("Config devices %v \n", string(d))
			//	devs := getDevices(ci.Config.Envs)
			devs, err := f.getDevices(podSb.Metadata.Name, ContStatusResp.Status.Metadata.Name, ci.Config.Envs)
			if err != nil {
				fmt.Errorf("unable to get device information from Pod Resource API %v", devs, err)
			}
			setCPU, err := cpuset.Parse(linuxResources.CPU.Cpus)
			if err != nil {
				fmt.Errorf("unable to parse %v as CPUSet: %v", linuxResources.CPU.Cpus, err)
			}
			cpus, err := getCPUs(setCPU)
			if err != nil {
				fmt.Errorf("unable to getCPU , err: %v", err)
			}
			resources := NewResources(cpus, devs)
			contData := NewContainerData(ContStatusResp.Status.Metadata.Name, resources)
			contsData = append(contsData, *contData)
			for i, c := range contsData {
				cdata, _ := json.Marshal(c)
				log.Printf("cdata[%v]: %v\n", i, string(cdata))
			}
		} //GetContainers ends here
		podData := NewPodResourceData(podSb.Id, podSb.Metadata.Uid, podSb.Metadata.Name, podSb.Metadata.Namespace, contsData)
		podResData = append(podResData, podData)
	} // getItems PodSbEnds here
	f.podsData = podResData
	return nil
}

func (f *finder) getDevices(podName, containerName string, envs []*runtime.KeyValue) (map[devicePluginResourceName][]*DeviceInfo, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//Pod Resource API client
	resp, err := f.podResourceClient.List(ctx, &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		return nil, fmt.Errorf("%v.Get(_) = _, %v", f.podResourceClient, err)
	}
	devices := make(map[devicePluginResourceName][]*DeviceInfo)
	for _, podResource := range resp.GetPodResources() {
		if podResource.Name != podName {
			continue
		}
		for _, container := range podResource.GetContainers() {
			if container.Name != containerName {
				continue
			}
			for _, device := range container.GetDevices() {
				for _, devId := range device.GetDeviceIds() {
					for _, env := range envs {
						if !strings.HasPrefix(env.Key, "example.com/device") {
							continue
						}
						k := strings.Split(env.Key, "_")
						if devId == k[1] {
							devInfo := NewDeviceInfo(device.ResourceName, devId, env.Value)
							var devPluginName devicePluginResourceName
							devPluginName = devicePluginResourceName(device.ResourceName)
							devices[devPluginName] = append(devices[devPluginName], devInfo)
						}
					}
				}
			}
		}
	}
	return devices, nil
}

func getCPUs(setCPU cpuset.CPUSet) (map[string]string, error) {
	cpuInfo := make(map[string]string)
	for _, cpu := range setCPU.ToSlice() {
		pathSuffix := fmt.Sprintf("bus/cpu/devices/cpu%d", cpu)
		cpuPath := path.Join(hostSysFs, pathSuffix)
		cpuDirs, err := ioutil.ReadDir(cpuPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CPU directory %v:%v ", cpuDirs, err)
		}
		for _, cpuDir := range cpuDirs {
			if !strings.HasPrefix(cpuDir.Name(), "node") {
				continue
			}
			numaNodeId := strings.TrimPrefix(cpuDir.Name(), "node")
			cpuInfo[fmt.Sprintf("%d", cpu)] = numaNodeId
		}
	}
	return cpuInfo, nil
}
