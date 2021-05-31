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

// puts the current kv pair into the current store if the key-to-shard mapping strategy
// is the current shard id, otherwise, forward it to one of the nodes with the hashed shard id
func ShardPutStore(s *SharedShardInfo, store map[string]StoreVal, currReplica string) {
	var d StoreVal
	s.Router.PUT("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")
		body := c.Request.Body

		data, _ := ioutil.ReadAll(body)
		// strBody := string(data[:])
		json.Unmarshal(data, &d)
		defer c.Request.Body.Close()

		shardId := Hash(currReplica) % uint32(s.ShardCount)

		if len(d.CausalMetadata) == 0 {
			d.CausalMetadata = []int{0, 0, 0, 0}
		}

		fmt.Printf("***CHECK shardId, s.CurrentShard, d.Val, & d.causalmetadata: %v, %v, (%v, %v)****", shardId, s.CurrentShard, d.Value, d.CausalMetadata)

		// if the newShardId matches that of the current node's shard id, then we can add to the store, & reply back
		if shardId == uint32(s.CurrentShard) {
			if _, exists := store[key]; exists {
				Mu.Mutex.Lock()
				store[key] = StoreVal{d.Value, d.CausalMetadata}
				Mu.Mutex.Unlock()
				c.JSON(http.StatusOK, gin.H{"message": "Added successfully", "causal-metadata": "<put-casual_metadata here>", "shard-id": s.CurrentShard})
			} else {
				Mu.Mutex.Lock()
				store[key] = StoreVal{d.Value, d.CausalMetadata}
				Mu.Mutex.Unlock()
				fmt.Printf("******CURRENT STORE: %v**********", store)
				c.JSON(http.StatusCreated, gin.H{"message": "Updated successfully", "causal-metadata": "<put-casual_metadata here>", "shard-id": s.CurrentShard})
			}
		} else { // otherwise we must create a new request and forward it to one of the members with the given <shard-id>
			Mu.Mutex.Lock()
			fmt.Printf("*********s.shardMembers[shardId] IN ELSE: %v******", s.ShardMembers[shardId])
			for index, member := range s.ShardMembers[shardId] {
				if member == currReplica {
					continue
				}

				fwdRequest, err := http.NewRequest("PUT", "http://"+s.ShardMembers[shardId][index]+"/key-value-store/"+key, bytes.NewBuffer(data))
				fmt.Printf("********BODY BEING SENT: %v********", string(data[:]))

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
				jsonData := json.RawMessage(body)
				c.JSON(response.StatusCode, jsonData)
				break // if we managed to receive a response back after forwarding, don't forward to other nodes in that same shard
			}
			Mu.Mutex.Unlock()
		}
		// TODO: once the response message is sent back to the client, we must replicate the key-value pair received across all members in the current shard
		// send nodes in the current shard the key as well
		Mu.Mutex.Lock()
		if shardId == uint32(s.CurrentShard) {
			for _, member := range s.ShardMembers[s.CurrentShard] {
				if member == currReplica { // don't send a PUT request to self
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

				// Mu.Mutex.Unlock()
				fwdRequest.Header = c.Request.Header

				httpForwarder := &http.Client{Timeout: 5 * time.Second}
				response, err := httpForwarder.Do(fwdRequest)
				// Mu.Mutex.Lock()

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

func canDeliver(senderVC []int, replicaVC []int) bool {
	// conditions for delivery:
	//      senderVC[senderslot] = replicaVC[senderslot] + 1
	//      senderVC[notsender] <= replicaVC[not sender]
	// r1: [1,0,0]
	// r2: [0,1,0] "bar"
	// from r2: [0,1,0]
	senderID := senderVC[3] // sender position in VC

	for i := 0; i < 3; i++ {
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
func updateVC(senderVC []int, replicaVC []int) []int {
	newVC := make([]int, 4)
	for i := 0; i < 3; i++ {
		fmt.Printf("SENDERVC: %v\n", senderVC)
		fmt.Printf("REPLICAVC: %v\n", replicaVC)
		newVC[i] = max(senderVC[i], replicaVC[i])
	}
	return newVC
}

//compareVC
//which clock is bigger/max? which can we use?
//return sum total of vector clock
func compareVC(leftVC []int, rightVC []int) []int {
	leftSum := 0
	rightSum := 0
	for i := 0; i < 2; i++ {
		leftSum += leftVC[i]
		rightSum += rightVC[i]
	}
	if leftSum > rightSum {
		return leftVC
	}
	return rightVC
}

func updateKvStore(view []string, dict map[string]StoreVal, currVC []int, s *SharedShardInfo) {
	//get updated kvstore from other replicas
	newStoreVal := make(map[string]StoreVal)
	for i := 0; i < len(s.ShardMembers[s.CurrentShard]); i++ {
		newStoreVal = KvGet(s.ShardMembers[s.CurrentShard][i])
		if len(newStoreVal) > 0 {
			break
		}
	}
	//Update local vector clock
	for _, value := range newStoreVal {
		currVC = compareVC(currVC, value.CausalMetadata)
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

// TODO : same as GET -
// 	determine if key is in this shardID
// 	if so, serve request
// 	else, forward request to a node in correct shard
// 	if key DNE in any, return error
//PutRequest for client interaction
// func PutRequest(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int) {
// 	var d StoreVal
// 	//receive request
// 	r.PUT("/key-value-store/:key", func(c *gin.Context) {
// 		key := c.Param("key")
// 		body, _ := ioutil.ReadAll(c.Request.Body)
// 		strBody := string(body[:])
// 		println("BODY: " + strBody)
// 		//hmmmm
// 		json.Unmarshal(body, &d)
// 		defer c.Request.Body.Close()
// 		if strBody == "{}" {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in PUT"})
// 		} else if len(key) > keyLimit {
// 			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in PUT"})
// 		} else {
// 			//If causal metadata is sent from client, we need to update the KVStore/check if we can deliver
// 			//Assume we can't so just update each time
// 			//[1,0,0] value:bar
// 			if len(d.CausalMetadata) > 0 {
// 				updateKvStore(view, dict, currVC)
// 			} else if len(d.CausalMetadata) == 0 {
// 				d.CausalMetadata = []int{0, 0, 0}
// 			}
// 			// increment on receive so we send back correct causal clock
// 			d.CausalMetadata[localAddr]++
// 			d.CausalMetadata = append(d.CausalMetadata, localAddr) //Index of sender address
// 			currVC = d.CausalMetadata

// 			if _, exists := dict[key]; exists {
// 				Mu.Mutex.Lock()
// 				dict[key] = StoreVal{d.Value, d.CausalMetadata}
// 				Mu.Mutex.Unlock()
// 				c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata[0:3]})
// 			} else { // otherwise we insert a new key-value pair //
// 				Mu.Mutex.Lock()
// 				dict[key] = StoreVal{d.Value, d.CausalMetadata}
// 				Mu.Mutex.Unlock()
// 				c.JSON(http.StatusCreated, gin.H{"message": "Added successfully", "replaced": false, "causal-metadata": d.CausalMetadata[0:3]})
// 			}
// 		}
// 		//send replicas PUT as well
// 		for i := 0; i < len(view); i++ {
// 			println("Replicating message to: " + "http://" + view[i] + "/key-value-store-r/" + key)
// 			c.Request.URL.Host = view[i]
// 			c.Request.URL.Scheme = "http"
// 			data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
// 			jsonData, _ := json.Marshal(data)
// 			fwdRequest, err := http.NewRequest("PUT", "http://"+view[i]+"/key-value-store-r/"+key, bytes.NewBuffer(jsonData))
// 			if err != nil {
// 				http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
// 				return
// 			}

// 			fwdRequest.Header = c.Request.Header

// 			httpForwarder := &http.Client{Timeout: 5 * time.Second}
// 			httpForwarder.Do(fwdRequest)
// 		}

// 	})
// }

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
				if canDeliver(d.CausalMetadata, currVC) {
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				} else {
					//get updated kvstore from other replicas
					updateKvStore(view, dict, currVC, s)
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				}
			} else { // otherwise we insert a new key-value pair //
				if canDeliver(d.CausalMetadata, currVC) {
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					Mu.Mutex.Lock()
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					Mu.Mutex.Unlock()
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				} else {
					updateKvStore(view, dict, currVC, s)
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
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
