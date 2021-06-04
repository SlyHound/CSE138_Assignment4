package utility

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
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
		key := c.Param("key")

		data, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(data[:])
		json.Unmarshal(data, &d)
		defer c.Request.Body.Close()

		
		shardId := HashModN(key, s.ShardCount)
		fmt.Printf("****** SHARDID FOR GET REQUEST: %v\n", shardId)
		fmt.Printf("****** KEY FOR GET REQUEST: %v\n", key)

		if strBody == "{}" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in GET"})
		} else if len(key) > keyLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in GET"})
		} else {
			// have to forward request to the first available replica in the correct shard
			for index, member := range s.ShardMembers[shardId] {
				if member == view.SocketAddr {
					if value, exists := store[key]; exists {
						// if the key is in this replica, just return it
						c.JSON(http.StatusOK, gin.H{"doesExist": true, "message": "Retrieved successfully", "value": value.Value, "causal-metadata": value.CausalMetadata})
						break
					} else {
						// otherwise, check other replicas in the shard
						continue
					}
				} else{ 
					data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
					jsonData, _ := json.Marshal(data)
					fwdRequest, err := http.NewRequest("GET", "http://"+s.ShardMembers[shardId][index]+"/key-value-store/"+key, bytes.NewBuffer(jsonData))
					//fmt.Printf("********DATA BEING SENT: %v********", data)
	
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
