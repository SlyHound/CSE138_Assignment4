package utility

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
)

// adds a new node to a given shard //
func addNode(currentShardMembers []string, currentShard string, r *gin.Engine) {
	var d Body // Body struct defined in viewPut.go //
	r.PUT("/key-value-store-shard/reshard", func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}

		json.Unmarshal(body, &d)

		currentShardMembers = append(currentShardMembers, d.Address)

		c.JSON(http.StatusOK, gin.H{}) // sends back just the 200 status code with no message body
	})
}
