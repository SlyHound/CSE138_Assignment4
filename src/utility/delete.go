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

type Metadata struct {
	CausalMetadata []int `json:"causal-metadata"`
}

// TODO : same as GET -
//      determine if key is in this shardID
//      if so, serve request
//      else, forward request to a node in correct shard
//      if key DNE in any, return error
//DeleteRequest Client endpoint for deletions
func ShardDeleteStore(s *SharedShardInfo, view *View, store map[string]StoreVal, localAddr int, currVC []int) {
	var (
		d  StoreVal
		fn fromNode
	)
	s.Router.DELETE("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")

		data, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(data[:])
		json.Unmarshal(data, &d)
		defer c.Request.Body.Close()

		Mu.Mutex.Lock()
		shardId := HashModN(view.SocketAddr, s.ShardCount)
		Mu.Mutex.Unlock()

		if strBody == "{}" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in PUT"})
		} else if len(key) > keyLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in PUT"})
		} else {
			//If causal metadata is sent from client, we need to update the KVStore/check if we can deliver
			//Assume we can't so just update each time
			if len(d.CausalMetadata) > 0 {
				Mu.Mutex.Lock()
				updateKvStore(view.PersonalView, store, currVC, s)
				Mu.Mutex.Unlock()
			} else if len(d.CausalMetadata) == 0 {
				Mu.Mutex.Lock()
				d.CausalMetadata = make([]int, len(view.PersonalView))
				for index := range view.PersonalView {
					d.CausalMetadata[index] = 0
				}
				Mu.Mutex.Unlock()
			}
			// increment on receive so we send back correct causal clock
			d.CausalMetadata[localAddr]++
			d.CausalMetadata = append(d.CausalMetadata, localAddr) //Index of sender address
			currVC = d.CausalMetadata

			fmt.Printf("***CHECK shardId, s.CurrentShard, d.Val, & d.causalmetadata: %v, %v, (%v, %v)****", shardId, s.CurrentShard, d.Value, d.CausalMetadata)

			// if the newShardId matches that of the current node's shard id, then we can delete it from the store, & reply back
			if shardId == s.CurrentShard {
				if _, exists := store[key]; exists {
					Mu.Mutex.Lock()
					delete(store, key)
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Added successfully", "causal-metadata": d.CausalMetadata[0:len(view.PersonalView)], "shard-id": s.CurrentShard})
				} else { // otherwise return error //
					c.JSON(http.StatusNotFound, gin.H{"error": "Key does not exist", "message": "Error in DELETE"})
				}
			} else { // otherwise we must create a new request and forward it to one of the members with the given <shard-id>
				Mu.Mutex.Lock()
				fmt.Printf("*********s.shardMembers[shardId] IN ELSE: %v******", s.ShardMembers[shardId])
				for index, member := range s.ShardMembers[shardId] {
					if member == view.SocketAddr {
						continue
					}

					data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
					jsonData, _ := json.Marshal(data)
					fwdRequest, err := http.NewRequest("DELETE", "http://"+s.ShardMembers[shardId][index]+"/key-value-store/"+key, bytes.NewBuffer(jsonData))
					//fmt.Printf("********DATA BEING SENT: %v********", data)

					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{})
						break
					}

					Mu.Mutex.Unlock()
					httpForwarder := &http.Client{Timeout: 5 * time.Second}
					response, err := httpForwarder.Do(fwdRequest)
					Mu.Mutex.Lock()

					if err != nil { // if an error occurs, assume the node is dead, so continue attempting to send to another node in the provided shard
						continue
					}

					body, _ := ioutil.ReadAll(response.Body)
					defer response.Body.Close()
					// jsonData = json.RawMessage(body)
					json.Unmarshal(body, &fn)
					fmt.Printf("********CHECK BODY BEING SENT: %v********", string(body[:]))
					c.JSON(response.StatusCode, gin.H{"message": fn.Message, "causal-metadata": fn.CausalMetadata, "shard-id": fn.ShardId})
					break // if we managed to receive a response back after forwarding, don't forward to other nodes in that same shard
				}
				Mu.Mutex.Unlock()
			}
		}
		// send nodes in the current shard the key as well
		Mu.Mutex.Lock()
		if shardId == s.CurrentShard {
			for _, member := range s.ShardMembers[s.CurrentShard] {
				if member == view.SocketAddr { // don't send a PUT request to self
					continue
				}
				c.Request.URL.Host = member
				c.Request.URL.Scheme = "http"
				data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
				jsonData, _ := json.Marshal(data)
				fwdRequest, err := http.NewRequest("DELETE", "http://"+member+"/key-value-store-r/"+key, bytes.NewBuffer(jsonData))
				if err != nil {
					http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
					return
				}

				Mu.Mutex.Unlock()
				fwdRequest.Header = c.Request.Header

				httpForwarder := &http.Client{Timeout: 5 * time.Second}
				response, err := httpForwarder.Do(fwdRequest)
				Mu.Mutex.Lock()

				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{})
					break
				}

				defer response.Body.Close()
			}
		}
		Mu.Mutex.Unlock()
	})
}

//ReplicateDelete endpoint to replicate delete messages
func ReplicateDelete(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int, s *SharedShardInfo) {
	var m Metadata
	r.DELETE("/key-value-store-r/:key", func(c *gin.Context) {
		key := c.Param("key")
		body, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(body[:])
		fmt.Printf("STRBODY: %s\n", strBody)
		json.Unmarshal(body, &m)
		fmt.Printf("CAUSAL CLOCK VALUE: %v\n", m.CausalMetadata)
		// if the key-value pair exists, then delete it //
		if _, exists := dict[key]; exists {
			if canDeliver(m.CausalMetadata, currVC, view) {
				m.CausalMetadata = dict[key].CausalMetadata
				c.JSON(http.StatusOK, gin.H{"message": "Deleted successfully", "causal-metadata": m.CausalMetadata})
				Mu.Mutex.Lock()
				delete(dict, key)
				Mu.Mutex.Unlock()
			} else {
				updateKvStore(view, dict, currVC, s)
				m.CausalMetadata = updateVC(m.CausalMetadata, currVC, view)
				c.JSON(http.StatusOK, gin.H{"message": "Deleted successfully", "causal-metadata": m.CausalMetadata})
				Mu.Mutex.Lock()
				delete(dict, key)
				Mu.Mutex.Unlock()
			}

		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "Key does not exist", "message": "Error in DELETE"})
		}
	})
}
