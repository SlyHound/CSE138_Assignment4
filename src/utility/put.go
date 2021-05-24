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

//implement way to determine clock is out of date (compareVC)
//call kvGet() if so, no need to sleep
//

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

func updateKvStore(view []string, dict map[string]StoreVal, currVC []int) {
	//get updated kvstore from other replicas
	newStoreVal := make(map[string]StoreVal)
	for i := 0; i < len(view); i++ {
		newStoreVal = KvGet(view[i])
		if len(newStoreVal) > 0 {
			break
		}
	}
	//Update local vector clock
	for _, value := range newStoreVal {
		currVC = compareVC(currVC, value.CausalMetadata)
	}
	//replace our KVStore with the new one if we get a change
	for key, storeVal := range newStoreVal {
		_, exists := dict[key]
		if !exists { // if the key doesn't exist in the store, then add it
			dict[fmt.Sprint(key)] = StoreVal{Value: storeVal.Value, CausalMetadata: storeVal.CausalMetadata}
		}
	}
}

//PutRequest for client interaction
func PutRequest(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int) {
	var d StoreVal
	//receive request
	r.PUT("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")
		body, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(body[:])
		println("BODY: " + strBody)
		//hmmmm
		json.Unmarshal(body, &d)
		defer c.Request.Body.Close()
		if strBody == "{}" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Value is missing", "message": "Error in PUT"})
		} else if len(key) > keyLimit {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Key is too long", "message": "Error in PUT"})
		} else {
			//If causal metadata is sent from client, we need to update the KVStore/check if we can deliver
			//Assume we can't so just update each time
			//[1,0,0] value:bar
			if len(d.CausalMetadata) > 0 {
				updateKvStore(view, dict, currVC)
			} else if len(d.CausalMetadata) == 0 {
				d.CausalMetadata = []int{0, 0, 0}
			}
			// increment on receive so we send back correct causal clock
			d.CausalMetadata[localAddr]++
			d.CausalMetadata = append(d.CausalMetadata, localAddr) //Index of sender address
			currVC = d.CausalMetadata
			//replace old value
			if _, exists := dict[key]; exists {
				//Causal CHECK @Jackie
				dict[key] = StoreVal{d.Value, d.CausalMetadata}
				//increment here
				c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata[0:3]})
			} else { // otherwise we insert a new key-value pair //
				dict[key] = StoreVal{d.Value, d.CausalMetadata}
				//increment here
				c.JSON(http.StatusCreated, gin.H{"message": "Added successfully", "replaced": false, "causal-metadata": d.CausalMetadata[0:3]})
			}
		}
		//send replicas PUT as well
		for i := 0; i < len(view); i++ {
			//TODO
			//refactor to skip vs remove in VC
			//Causal INCREMENT @Jackie
			println("Replicating message to: " + "http://" + view[i] + "/key-value-store-r/" + key)
			c.Request.URL.Host = view[i]
			c.Request.URL.Scheme = "http"
			data := &StoreVal{Value: d.Value, CausalMetadata: d.CausalMetadata}
			jsonData, _ := json.Marshal(data)
			fwdRequest, err := http.NewRequest("PUT", "http://"+view[i]+"/key-value-store-r/"+key, bytes.NewBuffer(jsonData))
			if err != nil {
				http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
				return
			}

			fwdRequest.Header = c.Request.Header

			httpForwarder := &http.Client{Timeout: 5 * time.Second}
			fwdResponse, err := httpForwarder.Do(fwdRequest)
			_ = fwdResponse

			// Shouldn't worry about Error checking? just send requests out and if things are down oh well?
			//TODO
			//USE THIS CATCH TO SEE IF SERVER IS DOWN AND UPDATE IN VIEW @Alex
			// if err != nil {
			// 	msg := "Error in " + fwdRequest.Method
			// 	c.JSON(http.StatusServiceUnavailable, gin.H{"error": view[i] + " is down", "message": msg})
			// }
			// if fwdResponse != nil {
			// 	body, _ := ioutil.ReadAll(fwdResponse.Body)
			// 	rawJSON := json.RawMessage(body)
			// 	c.JSON(fwdResponse.StatusCode, rawJSON)
			// 	defer fwdResponse.Body.Close()
			// }
		}

	})
}

//ReplicatePut Endpoint for replication
func ReplicatePut(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int) {
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
			// TO-DO: implement causal consistency and compare causal-metadata here
			if _, exists := dict[key]; exists {
				if canDeliver(d.CausalMetadata, currVC) {
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				} else {
					//get updated kvstore from other replicas
					updateKvStore(view, dict, currVC)
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				}
			} else { // otherwise we insert a new key-value pair //
				if canDeliver(d.CausalMetadata, currVC) {
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				} else {
					updateKvStore(view, dict, currVC)
					d.CausalMetadata = updateVC(d.CausalMetadata, currVC)
					currVC = d.CausalMetadata
					dict[key] = StoreVal{d.Value, d.CausalMetadata}
					c.JSON(http.StatusOK, gin.H{"message": "Updated successfully", "replaced": true, "causal-metadata": d.CausalMetadata})
				}
			}
		}
	})
}
