package utility

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type numKeys struct {
	Message  string `json:"message"`
	KeyCount int    `json:"shard-id-key-count"`
}

type SharedShardInfo struct {
	CurrentShard int                 // the current node's shard
	ShardMembers [][]string          // contains all members of each shard at each index
	ShardCount   int                 // total amount of shards we must have
	MinNodes     int                 // Amount of nodes in each shard (given by the /reshard command, default is 2)
	LocalKVStore map[string]StoreVal // Keep reference to KVStore so we can update it if needed
	Router       *gin.Engine
}

// helper function that returns members of a given shardId
func GetMembersOfShard(s *SharedShardInfo, shardId int) []string {
	members := s.ShardMembers[0] // default to the first shard

	// find the slice containing the shard members of the particular matching shardId's //
	for shardIndex := range s.ShardMembers {
		if shardId == shardIndex+1 { // note: index + 1 since index starts at 0, but we want to start at 1
			members = s.ShardMembers[shardIndex]
			break
		}
	}
	return members
}

// gets all the shard ids that currently exist (note: a JSON array of integers is returned)
func GetAllShardIds(s *SharedShardInfo) {
	s.Router.GET("/key-value-store-shard/shard-ids", func(c *gin.Context) {
		shardIds := make([]int, len(s.ShardMembers))

		// adds shard ids to the shardIds slice to then be returned //
		for index := range s.ShardMembers {
			shardIds = append(shardIds, index+1) // note: index + 1 since index starts at 0, but we want to start at 1
		}

		c.JSON(http.StatusOK, gin.H{"message": "Shard IDs retrieved successfully", "shard-ids": shardIds})
	})
}

// gets the current node's shard id (note: the shard id is an integer)
func GetNodeShardId(s *SharedShardInfo) {
	s.Router.GET("/key-value-store-shard/node-shard-id", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "Shard ID of the node retrieved successfully", "shard-id": s.CurrentShard})
	})
}

// gets the number of keys stored in a given shard
func GetNumKeys(kvStore map[string]StoreVal, s *SharedShardInfo) {
	var nk numKeys
	s.Router.GET("/key-value-store-shard/shard-id-key-count/:id", func(c *gin.Context) {
		shardId := c.Param("id")
		sentShardId, _ := strconv.Atoi(shardId)

		if sentShardId == s.CurrentShard {
			c.JSON(http.StatusOK, gin.H{"message": "Key count of shard ID retrieved successfully", "shard-id-key-count": len(kvStore)})
		} else { // otherwise request the length of the key value store of the other shard
			members := GetMembersOfShard(s, sentShardId)

			for _, member := range members {
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

// gets the members of a given shard based on the provided ID
func GetMembers(s *SharedShardInfo) {
	s.Router.GET("key-value-store-shard/shard-id-members/:id", func(c *gin.Context) {
		shardId := c.Param("id")

		sentShardId, _ := strconv.Atoi(shardId)
		members := GetMembersOfShard(s, sentShardId)

		c.JSON(http.StatusOK, gin.H{"message": "Members of shard ID retrieved successfully", "shard-id-members": members})
	})
}
