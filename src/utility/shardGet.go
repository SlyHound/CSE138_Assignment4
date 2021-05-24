package utility

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type numKeys struct {
	Message  string `json:"message"`
	KeyCount int    `json:"shard-id-key-count"`
}

// gets the number of keys stored in a given shard //
func GetNumKeys(kvStore map[string]StoreVal, currentShard string, otherShardMembers []string, r *gin.Engine) {
	var nk numKeys
	r.GET("/key-value-store-shard/shard-id-key-count/:id", func(c *gin.Context) {
		shardId := c.Param("id")

		if shardId == currentShard {
			c.JSON(http.StatusOK, gin.H{"message": "Key count of shard ID retrieved successfully", "shard-id-key-count": len(kvStore)})
		} else { // otherwise request the length of the key value store of the other shard
			for _, member := range otherShardMembers {
				request, err := http.NewRequest("GET", "http://"+member+"/key-value-store-shard/shard-id-key-count/"+shardId, nil)

				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue creating a http request"})
				}

				request.Header.Set("Content-Type", "application/json")
				httpForwarder := &http.Client{} // alias for DefaultClient
				response, err := httpForwarder.Do(request)

				// if an error occurred attempting to forward the request, then continue attempting to send to another member of the other shard //
				if err != nil {
					continue
				}
				defer response.Body.Close()
				body, _ := ioutil.ReadAll(response.Body)
				strBody := string(body[:])
				json.NewDecoder(strings.NewReader(strBody)).Decode(&nk)

				c.JSON(http.StatusOK, gin.H{"message": "Key count of shard ID retrieved successfully", "shard-id-key-count": nk.KeyCount})
				break // break after being able to send to response back regarding other shards key-value pair length //
			}
		}
	})
}

// gets the members of a given shard based on the provided ID //
func GetMembers(currentShardMembers []string, otherShardMembers []string, currentShard string, r *gin.Engine) {
	r.GET("key-value-store-shard/shard-id-members/:id", func(c *gin.Context) {
		shardId := c.Param("id")

		if shardId == currentShard {
			c.JSON(http.StatusOK, gin.H{"message": "Members of shard ID retrieved successfully", "shard-id-members": currentShardMembers})
		} else {
			c.JSON(http.StatusOK, gin.H{"message": "Members of shard ID retrieved successfully", "shard-id-members": otherShardMembers})
		}
	})
}
