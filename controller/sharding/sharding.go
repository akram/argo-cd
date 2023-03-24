package sharding

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"

	"github.com/argoproj/argo-cd/v2/pkg/apis/application/v1alpha1"
	log "github.com/sirupsen/logrus"
)

func InferShard() (int, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return 0, err
	}
	parts := strings.Split(hostname, "-")
	if len(parts) == 0 {
		return 0, fmt.Errorf("hostname should ends with shard number separated by '-' but got: %s", hostname)
	}
	shard, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return 0, fmt.Errorf("hostname should ends with shard number separated by '-' but got: %s", hostname)
	}
	return shard, nil
}

// GetShardByID calculates cluster shard as `clusterSecret.UID % replicas count`
func GetShardByID(id string, replicas int) int {
	log.Infof("Calculating cluster shard for cluster id: %s", id)
	if id == "" {
		return 0
	} else {
		h := fnv.New32a()
		_, _ = h.Write([]byte(id))
		return int(h.Sum32() % uint32(replicas))
	}
}

func GetClusterFilter(replicas int, shard int) func(c *v1alpha1.Cluster) bool {
	return func(c *v1alpha1.Cluster) bool {
		clusterShard := 0
		//  cluster might be nil if app is using invalid cluster URL, assume shard 0 in this case.
		if c != nil {
			log.Debugf("Processing cluster: id/name/shard: %s/%s/%d", c.ID, c.Name, &c.Shard)
			if c.Shard != nil {
				clusterShard = int(*c.Shard)
				log.Debugf("Value already set for cluster %s: shard: %d", c.Name, clusterShard)
			} else {
				clusterShard = GetShardByID(c.ID, replicas)
				log.Debugf("Calculated cluster shard %d for cluster %s:%s", shard, c.Name, c.Server)
			}
		}
		return clusterShard == shard
	}
}
