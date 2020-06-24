package finder

import (
	"context"

	v1 "k8s.io/api/core/v1"
	podresourcesapi "k8s.io/kubernetes/pkg/kubelet/apis/podresources/v1alpha1"
)

// DevicesProvider knows how to provide the devices used by the given container
type DevicesProvider interface {
	GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices
	UpdateAllocatedDevices()
}

// PodsProvider knows how to provide the pods admitted by the node
type PodsProvider interface {
	GetPods() []*v1.Pod
}

// podResourcesServer implements PodResourcesListerServer
type podResourcesServer struct {
	podsProvider    PodsProvider
	devicesProvider DevicesProvider
}

// NewPodResourcesServer returns a PodResourcesListerServer which lists pods provided by the PodsProvider
// with device information provided by the DevicesProvider
func NewPodResourcesServer(podsProvider PodsProvider, devicesProvider DevicesProvider) podresourcesapi.PodResourcesListerServer {
	return &podResourcesServer{
		podsProvider:    podsProvider,
		devicesProvider: devicesProvider,
	}
}

// List returns information about the resources assigned to pods on the node
func (p *podResourcesServer) List(ctx context.Context, req *podresourcesapi.ListPodResourcesRequest) (*podresourcesapi.ListPodResourcesResponse, error) {
	pods := p.podsProvider.GetPods()
	podResources := make([]*podresourcesapi.PodResources, len(pods))
	p.devicesProvider.UpdateAllocatedDevices()

	for i, pod := range pods {
		pRes := podresourcesapi.PodResources{
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			Containers: make([]*podresourcesapi.ContainerResources, len(pod.Spec.Containers)),
		}

		for j, container := range pod.Spec.Containers {
			pRes.Containers[j] = &podresourcesapi.ContainerResources{
				Name:    container.Name,
				Devices: p.devicesProvider.GetDevices(string(pod.UID), container.Name),
			}
		}
		podResources[i] = &pRes
	}

	return &podresourcesapi.ListPodResourcesResponse{
		PodResources: podResources,
	}, nil
}
