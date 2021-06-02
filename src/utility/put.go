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

const (
	keyLimit int = 50 // maximum number of characters allowed for a key
)

type StoreVal struct {
	Value          string `json:"value"`
	CausalMetadata []int  `json:"causal-metadata"`
}

// struct designed for decoding JSON data received from a node in a different shard
type fromNode struct {
	Message        string `json:"message"`
	CausalMetadata []int  `json:"causal-metadata"`
	ShardId        int    `json:"shard-id"`
}

func canDeliver(senderVC []int, replicaVC []int, view []string) bool {
	// conditions for delivery:
	//      senderVC[senderslot] = replicaVC[senderslot] + 1
	//      senderVC[notsender] <= replicaVC[not sender]
	senderID := senderVC[len(view)] // sender position in VC

	for i := 0; i < len(view); i++ {
		//if sender clock isn't updated by 1 more
		if i == senderID && senderVC[i] != replicaVC[i]+1 {
			return false
		} else if i != senderID && senderVC[i] > replicaVC[i] { //if something else than the sender incremented clock
			fmt.Println("canDeliver: WE CAN'T DELIVER!!")
			return false
		}
	}

	//Otherwise, our clock is only a difference of 1 in senderID slot between the currentVC and the senderVC
	return true
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

// calculate new VC: max(senderVC, replicaVC)
func updateVC(senderVC []int, replicaVC []int, view []string) []int {
	newVC := make([]int, len(view))
	for i := 0; i < len(view); i++ {
		fmt.Printf("SENDERVC: %v\n", senderVC)
		fmt.Printf("REPLICAVC: %v\n", replicaVC)
		newVC[i] = max(senderVC[i], replicaVC[i])
	}
	return newVC
}

//compareVC
//which clock is bigger/max? which can we use?
//return sum total of vector clock
func compareVC(leftVC []int, rightVC []int, view []string) []int {
	leftSum := 0
	rightSum := 0
	for i := 0; i < len(view); i++ {
		leftSum += leftVC[i]
		rightSum += rightVC[i]
	}
	if leftSum > rightSum {
		return leftVC
	}
	return rightVC
}

func updateKvStore(view []string, dict map[string]StoreVal, currVC []int, s *SharedShardInfo) {
	//get updated kvstore from other nodes in the current shard
	newStoreVal := make(map[string]StoreVal)
	Mu.Mutex.Lock()
	for i := 0; i < len(s.ShardMembers[s.CurrentShard]); i++ {
		newStoreVal = KvGet(s.ShardMembers[s.CurrentShard][i])
		if len(newStoreVal) > 0 {
			break
		}
	}
	Mu.Mutex.Unlock()
	//Update local vector clock
	for _, value := range newStoreVal {
		currVC = compareVC(currVC, value.CausalMetadata, view)
	}
	Mu.Mutex.Lock()
	//replace our KVStore with the new one if we get a change
	for key, storeVal := range newStoreVal {
		_, exists := dict[key]
		if !exists { // if the key doesn't exist in the store, then add it
			dict[fmt.Sprint(key)] = StoreVal{Value: storeVal.Value, CausalMetadata: storeVal.CausalMetadata}
		}
	}
	Mu.Mutex.Unlock()
}

// puts the current kv pair into the current store if the key-to-shard mapping strategy
// is the current shard id, otherwise, forward it to one of the nodes with the hashed shard id
func ShardPutStore(s *SharedShardInfo, view *View, store map[string]StoreVal, localAddr int, currVC []int) {
	var (
		d  StoreVal
		fn fromNode
	)
	s.Router.PUT("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")
		// body := c.Request.Body

		data, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(data[:])
		json.Unmarshal(data, &d)
		defer c.Request.Body.Close()

		shardId := Hash(view.SocketAddr) % uint32(s.ShardCount)

		if strBody == "{}" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in PUT"})
		} else if len(key) > keyLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in PUT"})
		} else {
			//If causal metadata is sent from client, we need to update the KVStore/check if we can deliver
			//Assume we can't so just update each time
			if len(d.CausalMetadata) > 0 {
				updateKvStore(view.PersonalView, store, currVC, s)
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

			// if the newShardId matches that of the current node's shard id, then we can add to the store, & reply back
			if shardId == uint32(s.CurrentShard) {
				if _, exists := store[key]; exists {
					Mu.Mutex.Lock()
					store[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Added successfully", "causal-metadata": d.CausalMetadata[0:len(view.PersonalView)], "shard-id": s.CurrentShard})
				} else { // otherwise we insert a new key-value pair //
					Mu.Mutex.Lock()
					store[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					fmt.Printf("******CURRENT STORE: %v**********", store)
					c.JSON(http.StatusCreated, gin.H{"message": "Updated successfully", "causal-metadata": d.CausalMetadata[0:len(view.PersonalView)], "shard-id": s.CurrentShard})
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
					fwdRequest, err := http.NewRequest("PUT", "http://"+s.ShardMembers[shardId][index]+"/key-value-store/"+key, bytes.NewBuffer(jsonData))
					fmt.Printf("********DATA BEING SENT: %v********", data)

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
		if shardId == uint32(s.CurrentShard) {
			for _, member := range s.ShardMembers[s.CurrentShard] {
				if member == view.SocketAddr { // don't send a PUT request to self
					continue
				}
				c.Request.URL.Host = member
				c.Request.URL.Scheme = "http"
				data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
				jsonData, _ := json.Marshal(data)
				fwdRequest, err := http.NewRequest("PUT", "http://"+member+"/key-value-store-r/"+key, bytes.NewBuffer(jsonData))
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

//ReplicatePut Endpoint for replication
func ReplicatePut(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int, s *SharedShardInfo) {
	var d StoreVal
	r.PUT("/key-value-store-r/:key", func(c *gin.Context) {
		key := c.Param("key")
		body, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(body[:])
		fmt.Printf("STRBODY: %s\n", strBody)
		json.Unmarshal(body, &d)
		fmt.Printf("VALUE: %s\n", d.Value)
		fmt.Printf("CAUSAL METADATA REPLICATION: %v\n", d.CausalMetadata)
		defer c.Request.Body.Close()
		if strBody == "{}" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in PUT"})
		} else if len(key) > keyLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in PUT"})
		} else {
			// if a key-value pair already exists, then replace the old value //
			if _, exists := dict[key]; exists {
				if canDeliver(d.CausalMetadata, currVC, view) {
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC, view)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				} else {
					//get updated kvstore from other replicas
					updateKvStore(view, dict, currVC, s)
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC, view)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				}
			} else { // otherwise we insert a new key-value pair //
				if canDeliver(d.CausalMetadata, currVC, view) {
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC, view)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				} else {
					updateKvStore(view, dict, currVC, s)
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC, view)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				}
			}
		}
		fmt.Printf("*******AFTER REPLICATION STORE: %v*******", dict)
	})
}
