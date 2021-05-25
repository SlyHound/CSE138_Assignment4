package utility

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
)

// adds a new node to a given shard //
func addNode(s *SharedShardInfo) {
	var d Body // Body struct defined in viewPut.go //
	s.Router.PUT("/key-value-store-shard/add-member/:id", func(c *gin.Context) {
		shardId := c.Param("id")
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}

		json.Unmarshal(body, &d)

		if shardId == "1" {
			s.ShardOneMembers = append(s.ShardOneMembers, d.Address)
		} else {
			s.ShardTwoMembers = append(s.ShardTwoMembers, d.Address)
		}

		c.JSON(http.StatusOK, gin.H{}) // sends back just the 200 status code with no message body
	})
}
