package utility

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

//GetRequest for client interaction
func GetRequest(r *gin.Engine, dict map[string]StoreVal, local_addr int, view []string) {

	r.GET("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")

		// if the key-value pair exists, then just return it //
		if value, exists := dict[key]; exists {
			c.JSON(http.StatusOK, gin.H{"doesExist": true, "message": "Retrieved successfully", "value": value.Value, "causal-metadata": value.CausalMetadata[0:3]})
		} else {
			c.JSON(http.StatusNotFound, gin.H{"doesExist": false, "error": "Key does not exist", "message": "Error in GET"})
		}
	})

}
