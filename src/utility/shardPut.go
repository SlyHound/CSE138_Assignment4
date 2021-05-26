package utility

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// adds a new node to a given shard
func AddNode(s *SharedShardInfo) {
	var d Body // Body struct defined in viewPut.go //
	s.Router.PUT("/key-value-store-shard/add-member/:id", func(c *gin.Context) {
		shardId := c.Param("id")
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}

		json.Unmarshal(body, &d)

		index := 0
		for shardIndex := range s.ShardMembers {
			if shardId == strconv.Itoa(shardIndex+1) { // note: index + 1 since index starts at 0, but we want to start at 1
				index = shardIndex
				break
			}
		}

		s.ShardMembers[index] = append(s.ShardMembers[index], d.Address)

		c.JSON(http.StatusOK, gin.H{}) // sends back just the 200 status code with no message body
	})
}
