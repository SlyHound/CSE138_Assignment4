package utility

import (
	"net/http"
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

		shardId := hash(view.SocketAddr) % uint32(s.ShardCount)

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
