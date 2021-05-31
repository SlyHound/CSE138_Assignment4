package utility

import (
	"net/http"
<<<<<<< Updated upstream

	"github.com/gin-gonic/gin"
)

// returns shardID where key exists
func getShardIDofKey(key string) int {
	// TODO
	// Proposed approach: 
	// 1. check current KVstore
	// 	if value, exists := dict[key]; exists {
	//    if here, simple serve request
	// 2. if not, send request to one node in each shard to look for it
	//    each shard will return either the key or an error
	// 3. if one returns the key, return that to client
	//    if all return error, return error 
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
=======
	"github.com/gin-gonic/gin"

	"io/ioutil"
	"encoding/json"
	"fmt"
	"bytes"
	"time"
)

// struct designed for decoding JSON data received from a node in a different shard
type getNode struct {
	Message        string `json:"message"`
	CausalMetadata []int  `json:"causal-metadata"`
	Value          string `json:"value"`
}

func ShardGetStore(s *SharedShardInfo, view *View, store map[string]StoreVal, localAddr int, currVC []int) {

	var (
		d  StoreVal
		gn getNode
	)

	s.Router.GET("/key-value-store/:key", func(c *gin.Context) {
		fmt.Printf("******** starting get ********")
		key := c.Param("key")

		data, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(data[:])
		json.Unmarshal(data, &d)
		defer c.Request.Body.Close()

		shardId := Hash(view.SocketAddr) % uint32(s.ShardCount)

		if strBody == "{}" {
			fmt.Printf("********empty body********")
			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in GET"})
		} else if len(key) > keyLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in GET"})
		} else {
			fmt.Printf("********no error********")
			// if the newShardId matches that of the current node's shard id, then key should be in this shard (if it exists)
			if shardId == uint32(s.CurrentShard) {	
				fmt.Printf("********this replica serves request********")
				// if it exists, return
				if value, exists := store[key]; exists {
					c.JSON(http.StatusOK, gin.H{"doesExist": true, "message": "Retrieved successfully", "value": value.Value, "causal-metadata": value.CausalMetadata})
				} else {	// key should be in this shard, so it must not exist
					c.JSON(http.StatusNotFound, gin.H{"doesExist": false, "error": "Key does not exist", "message": "Error in GET"})
				}
			} else {
				fmt.Printf("********forward case ********")
				// have to forward request to the first available replica in the correct shard
				for index, member := range s.ShardMembers[shardId] {
					if member == view.SocketAddr {
						continue
					}

					data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
					jsonData, _ := json.Marshal(data)
					fwdRequest, err := http.NewRequest("GET", "http://"+s.ShardMembers[shardId][index]+"/key-value-store/"+key, bytes.NewBuffer(jsonData))
					fmt.Printf("********DATA BEING SENT: %v********", data)

					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{})
						break
					}

					httpForwarder := &http.Client{Timeout: 5 * time.Second}
					response, err := httpForwarder.Do(fwdRequest)

					// if err != nil{
					// right now must keep the status code version since replication is not implemented
					if err != nil || response.StatusCode != 200 { // if an error occurs, assume the node is dead, so continue attempting to send to another node in the provided shard
						continue
					}

					body, _ := ioutil.ReadAll(response.Body)
					defer response.Body.Close()
					// jsonData = json.RawMessage(body)
					json.Unmarshal(body, &gn)
					fmt.Printf("********CHECK BODY BEING SENT: %v********", string(body[:]))
					c.JSON(response.StatusCode, gin.H{"message": gn.Message, "causal-metadata": gn.CausalMetadata, "value": gn.Value})
					break // if we managed to receive a response back after forwarding, don't forward to other nodes in that same shard
				}
			}
		}	
	})
}
>>>>>>> Stashed changes
