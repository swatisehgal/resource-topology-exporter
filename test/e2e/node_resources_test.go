package e2e_test

import (
	"context"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/swatisehgal/resource-topology-exporter/test/e2e"
	//	v1alpha1 "github.com/swatisehgal/topologyapi/pkg/apis/topology/v1alpha1"
	clientset "github.com/swatisehgal/topologyapi/pkg/generated/clientset/versioned"
)

func findHostName(addresses []corev1.NodeAddress) string {
	for _, addr := range addresses {
		if addr.Type == corev1.NodeHostName {
			return addr.Address
		}
	}
	return ""
}

var _ = Describe("NodeResources", func() {
	Context("without any pod", func() {
		It("should report consistent values with node allocatable", func() {
			var err error

			nodes, err := e2e.GetByRole("worker")
			Expect(err).ToNot(HaveOccurred())

			clientConfig, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
			Expect(err).ToNot(HaveOccurred())

			cli, err := clientset.NewForConfig(clientConfig)
			Expect(err).ToNot(HaveOccurred())

			for _, node := range nodes {
				hostname := findHostName(node.Status.Addresses)
				Expect(hostname).NotTo(Equal(""))
				fmt.Fprintf(GinkgoWriter, "hostname = %v\n", hostname)

				for name, value := range node.Status.Allocatable {
					fmt.Fprintf(GinkgoWriter, "%v = %v\n", name, value)
				}

				// TODO: find namespace in a smart way
				nrt, err := cli.TopologyV1alpha1().NodeResourceTopologies("default").Get(context.TODO(), hostname, metav1.GetOptions{})
				Expect(err).ToNot(HaveOccurred())

				fmt.Fprintf(GinkgoWriter, "%v\n", nrt)

				// 3. check the node allocatable is "consistent" with the resources
			}
		})
	})
})
