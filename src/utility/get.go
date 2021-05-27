package utility

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// returns shardID where key exists
func getShardIDofKey(key string) int {
	// TODO
}

// returns shardID of current node
func thisShardID(localAddr int) int {
	// GetNodeShardId()
}

//GetRequest for client interaction
func GetRequest(r *gin.Engine, dict map[string]StoreVal, local_addr int, view []string) {

	r.GET("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")

		// WITH SHARDS: have to retrieve which shard the KV pair is in
		// if the key-value pair is in this shard, then just return it
		// otherwise, forward request to a node inside the correct shard
		// if we can;t find it in any shard, return error
		keyShardID := getShardIDofKey(key)	// TODO: find which shard the key is in
		if keyShardID == thisShardID(local_addr) {
//		if value, exists := dict[key]; exists {
			c.JSON(http.StatusOK, gin.H{"doesExist": true, "message": "Retrieved successfully", "value": value.Value, "causal-metadata": value.CausalMetadata[0:3]})
		} else if keyShardID != -1 {	// shardID exists, isnt this one
			// forward request to a replica in the correct shard

		} else {
			// if -1, then that means key does not have shardID
			// return "Key does not exist" error
			c.JSON(http.StatusNotFound, gin.H{"doesExist": false, "error": "Key does not exist", "message": "Error in GET"})
		}
	})

}
