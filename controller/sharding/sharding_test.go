package sharding

import (
	"os"
	"testing"

	"github.com/argoproj/argo-cd/v2/common"
	"github.com/argoproj/argo-cd/v2/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v2/test"

	"context"

	"github.com/argoproj/argo-cd/v2/util/db"
	"github.com/argoproj/argo-cd/v2/util/settings"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeData struct {
	apps          []runtime.Object
	configMapData map[string]string
}

func TestGetShardByID_NotEmptyID(t *testing.T) {
	os.Setenv(common.EnvControllerReplicas, "1")
	assert.Equal(t, 0, GetShardByIdUsingHashDistributionFunction()(&v1alpha1.Cluster{ID: "1"}))
	assert.Equal(t, 0, GetShardByIdUsingHashDistributionFunction()(&v1alpha1.Cluster{ID: "2"}))
	assert.Equal(t, 0, GetShardByIdUsingHashDistributionFunction()(&v1alpha1.Cluster{ID: "3"}))
	assert.Equal(t, 0, GetShardByIdUsingHashDistributionFunction()(&v1alpha1.Cluster{ID: "4"}))
}

func TestGetShardByID_EmptyID(t *testing.T) {
	os.Setenv(common.EnvControllerReplicas, "1")
	distributionFunction := GetShardByIdUsingHashDistributionFunction
	shard := distributionFunction()(&v1alpha1.Cluster{})
	assert.Equal(t, 0, shard)
}

func TestGetShardByID_NoReplicas(t *testing.T) {
	os.Setenv(common.EnvControllerReplicas, "0")
	distributionFunction := GetShardByIdUsingHashDistributionFunction
	shard := distributionFunction()(&v1alpha1.Cluster{})
	assert.Equal(t, -1, shard)
}

func TestGetClusterFilter(t *testing.T) {
	shardIndex := 1 // ensuring that a shard with index 1 will process all the clusters with an "even" id (2,4,6,...)
	os.Setenv(common.EnvControllerReplicas, "2")
	filter := GetClusterFilter(GetDistributionFunction(nil, nil), shardIndex)
	assert.False(t, filter(&v1alpha1.Cluster{ID: "1"}))
	assert.True(t, filter(&v1alpha1.Cluster{ID: "2"}))
	assert.False(t, filter(&v1alpha1.Cluster{ID: "3"}))
	assert.True(t, filter(&v1alpha1.Cluster{ID: "4"}))
}

func TestGetShardByIndexModuloReplicasCountDistributionFunction2(t *testing.T) {
	os.Setenv(common.EnvControllerReplicas, "2")
	ctx := context.Background()
	settingsMgr, clientset := newFakeClient(ctx)
	distributionFunction := GetShardByIndexModuloReplicasCountDistributionFunction(settingsMgr, clientset)
	db := db.NewDB(settingsMgr.GetNamespace(), settingsMgr, clientset)

	cluster1 := createAndSaveCluster("cluster1", "123", db, ctx, t)
	cluster2 := createAndSaveCluster("cluster2", "456", db, ctx, t)
	cluster3 := createAndSaveCluster("cluster3", "789", db, ctx, t)
	cluster4 := createAndSaveCluster("cluster4", "abc", db, ctx, t)
	cluster5 := createAndSaveCluster("cluster5", "def", db, ctx, t)

	// Test with replicas set to 1
	os.Setenv(common.EnvControllerReplicas, "1")
	assert.Equal(t, 1, distributionFunction(&cluster1))
	assert.Equal(t, 1, distributionFunction(&cluster2))
	assert.Equal(t, 1, distributionFunction(&cluster3))
	assert.Equal(t, 1, distributionFunction(&cluster4))
	assert.Equal(t, 1, distributionFunction(&cluster5))

	// Test with replicas set to 2
	os.Setenv(common.EnvControllerReplicas, "2")
	assert.Equal(t, 0, distributionFunction(&cluster1))
	assert.Equal(t, 1, distributionFunction(&cluster2))
	assert.Equal(t, 0, distributionFunction(&cluster3))
	assert.Equal(t, 1, distributionFunction(&cluster4))
	assert.Equal(t, 0, distributionFunction(&cluster5))

	// Test with replicas set to 3
	os.Setenv(common.EnvControllerReplicas, "3")
	assert.Equal(t, 0, distributionFunction(&cluster1))
	assert.Equal(t, 1, distributionFunction(&cluster2))
	assert.Equal(t, 2, distributionFunction(&cluster3))
	assert.Equal(t, 0, distributionFunction(&cluster4))
	assert.Equal(t, 1, distributionFunction(&cluster5))
}

func TestGetShardByIndexModuloReplicasCountDistributionFunction(t *testing.T) {
	os.Setenv(common.EnvControllerReplicas, "2")
	ctx := context.Background()
	settingsMgr, clientset := newFakeClient(ctx)
	db := db.NewDB(settingsMgr.GetNamespace(), settingsMgr, clientset)

	cluster1 := createAndSaveCluster("cluster1", "123", db, ctx, t)
	cluster2 := createAndSaveCluster("cluster2", "456", db, ctx, t)
	distributionFunction := GetShardByIndexModuloReplicasCountDistributionFunction(settingsMgr, clientset)

	// Test that the function returns the correct shard for cluster1 and cluster2
	expectedShardForCluster1 := 0
	expectedShardForCluster2 := 1
	shardForCluster1 := distributionFunction(&cluster1)
	shardForCluster2 := distributionFunction(&cluster2)

	if shardForCluster1 != expectedShardForCluster1 {
		t.Errorf("Expected shard for cluster1 to be %d but got %d", expectedShardForCluster1, shardForCluster1)
	}
	if shardForCluster2 != expectedShardForCluster2 {
		t.Errorf("Expected shard for cluster2 to be %d but got %d", expectedShardForCluster2, shardForCluster2)
	}
}

func createAndSaveCluster(name string, id string, db db.ArgoDB, ctx context.Context, t *testing.T) v1alpha1.Cluster {
	cluster1 := v1alpha1.Cluster{
		Name:   name,
		ID:     id,
		Server: "https://kubernetes.default.svc?" + id,
	}

	_, err := db.CreateCluster(ctx, &cluster1)
	if err != nil {
		t.Fatalf("Error creating cluster 1: %v", err)
	}
	return cluster1
}

func newFakeClient(ctx context.Context) (*settings.SettingsManager, kubernetes.Interface) {
	data := &fakeData{apps: []runtime.Object{}}

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "argocd-secret",
			Namespace: test.FakeArgoCDNamespace,
		},
		Data: map[string][]byte{
			"admin.password":   []byte("test"),
			"server.secretkey": []byte("test"),
		},
	}
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "argocd-cm",
			Namespace: test.FakeArgoCDNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/part-of": "argocd",
			},
		},
		Data: data.configMapData,
	}
	var clust corev1.Secret
	kubeClientset := fake.NewSimpleClientset(&clust, &cm, &secret)
	settingsMgr := settings.NewSettingsManager(ctx, kubeClientset, test.FakeArgoCDNamespace)
	return settingsMgr, kubeClientset
}
