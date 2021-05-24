package utility

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// gets the number of keys stored in a given shard //
func GetNumKeys(kvStore map[string]StoreVal, r *gin.Engine) {
	r.GET("/key-value-store-shard/shard-id-key-count/:id", func(c *gin.Context) {
		// shardId := c.Param("id") // not really sure what to use the shardId for

		c.JSON(http.StatusOK, gin.H{"message": "Key count of shard ID retrieved successfully", "shard-id-key-count": len(kvStore)})
	})
}

// gets the members of a given shard based on the provided ID //
func GetMembers(members []string, r *gin.Engine) {
	r.GET("key-value-store-shard/shard-id-members/:id", func(c *gin.Context) {
		// shardId := c.Param("id") // not really sure what to use the shardId for

		c.JSON(http.StatusOK, gin.H{"message": "Members of shard ID retrieved successfully", "shard-id-members": members})
	})
}
