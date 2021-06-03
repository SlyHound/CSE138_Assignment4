package utility

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
)

type ShardMsg struct {
	ShardCount int `json:"shard-count"`
}

type chunkBody struct {
	ChunkInfo map[string]StoreVal `json:"chunk-info"`
}
type newShards struct {
	NewShards [][]string `json:"shard-members"`
}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

//Route to determine resharding request parameters
func ReshardRoute(view *View, shards *SharedShardInfo) {
	var ns ShardMsg //newShard request
	shards.Router.PUT("/key-value-store-shard/reshard", func(c *gin.Context) {
		body, _ := ioutil.ReadAll(c.Request.Body)
		println(string(body[:]))
		err := json.Unmarshal(body, &ns)
		if err != nil {
			println("ERROR IN UNMARSHALLING")
			println(err)
		}
		//Check if shard request has something in it, otherwise error
		defer c.Request.Body.Close()
		Mu.Mutex.Lock()
		println(len(view.PersonalView))
		println(len(view.PersonalView) / ns.ShardCount)
		if len(view.PersonalView)/ns.ShardCount < 2 {
			c.JSON(http.StatusBadRequest, gin.H{"message": "Not enough nodes to provide fault-tolerance with the given shard count!"})
		} else {
			reshard(view, shards, c)
		}
		Mu.Mutex.Unlock()
	})

}

//Route for chunk replication for new shard data
func ChunkRoute(s *SharedShardInfo) {
	var b chunkBody
	s.Router.PUT("/key-value-store-shard/chunk-r", func(c *gin.Context) {
		//Add everything sent in request to local storage
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}
		json.Unmarshal(body, &b)
		//replace whole chunk
		s.LocalKVStore = b.ChunkInfo

		c.JSON(http.StatusOK, gin.H{})
	})
}

//Route for updating shard members
func UpdateShardMembersRoute(s *SharedShardInfo) {
	var d newShards
	s.Router.PUT("/key-value-store-shard/updatesm", func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}
		json.Unmarshal(body, &d)
		s.ShardMembers = d.NewShards
		fmt.Printf("********* NEWSHARD JSON %v\n", d.NewShards)
		fmt.Printf("********* NEWSHARD MEMBERS %v\n", s.ShardMembers)

		c.JSON(http.StatusOK, gin.H{})
	})

}

//function to determine what shard we should place k-v pair in
func HashModN(s string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() % uint32(n))
}

//Actual resharding algorithm
//LOCK THIS WHOLE FUNCTION
func reshard(view *View, shards *SharedShardInfo, c *gin.Context) {
	//Basic strategy is brute force
	//hash IP address, mod N, place in there
	//first GET preReshard state (store current shard members/view)
	// need to redistribute the KVStore evenly as well
	// First step to redistribute the replicas and the 2nd step is to rehash the data
	// Create Map of Maps to be our "chunk" to send to the new shards
	chunks := make(map[int]map[string]StoreVal)
	newShardMembers := [][]string{}
	//First for loop builds newShardMembers
	//first sort view before splitting into shards

	sort.Strings(view.PersonalView)
	toBeSharded := view.PersonalView
	evenSplit := len(view.PersonalView) / shards.ShardCount //keeps general count for how many replicas we should have in each shard
	currShardID := 0
	// while we have replicas to "shard"
	fmt.Printf("******** TOBESHARDED: %v\n", toBeSharded)
	for len(toBeSharded) > 0 {
		//move to next shard if we have at least an even split in our current one, otherwise add to currentShard
		fmt.Printf("******** NEWSHARDMEMBERS: %v\n", newShardMembers)
		fmt.Printf("******** currShardID: %v\n", currShardID)
		newShardMembers[currShardID] = append(newShardMembers[currShardID], toBeSharded[0])
		toBeSharded = toBeSharded[1:]
		if len(newShardMembers[currShardID]) >= evenSplit {
			currShardID++
		} else if currShardID > shards.ShardCount-1 {
			//If we have already done an even split and there's a remainder of our replicas, just append to last shard and then make array
			newShardMembers[currShardID] = append(newShardMembers[currShardID-1], toBeSharded...)
			toBeSharded = nil //or break
		}
	}

	//Map old IDs so we don't need to worry about things
	oldShardIDs := make(map[string]int)
	for idx, row := range shards.ShardMembers {
		for _, col := range row {
			oldShardIDs[col] = idx
		}
	}

	//going from SC=2 -> SC=3
	for currentShard := 0; currentShard < len(shards.ShardMembers); currentShard++ {
		//for each node, hash IP and then mod amount of shards given by our /reshard call
		//GET the current shard KVStore for each shard, determine what needs to be rehashed and put in a chunk,
		//Otherwise keep the store as such
		shardKV := KvGet(shards.ShardMembers[currentShard][0])
		oldShardID := currentShard
		for key, value := range shardKV {
			newShardID := HashModN(key, shards.ShardCount)
			chunks[newShardID][key] = value
			if newShardID != oldShardID {
				//Delete from the original shardKV
				delete(shardKV, key)
			}
		}

	}

	fmt.Printf("********* CURRENTSHARD MEMBERS %v\n", shards.ShardMembers)
	fmt.Printf("********* NEWSHARD MEMBERS %v\n", newShardMembers)
	//broadcast new shards out to every replica
	shards.ShardMembers = newShardMembers
	//second for loop, broadcasts newShardMembers to every replica in the view
	updatedShards := newShards{
		NewShards: newShardMembers,
	}
	client := &http.Client{}
	usJSON, err := json.Marshal(updatedShards)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error in resharding process"})
	}
	for i := 0; i < len(view.PersonalView); i++ {
		req, err := http.NewRequest(http.MethodPut, "http://"+view.PersonalView[i]+"/key-value-store-shard/updatesm", bytes.NewBuffer(usJSON))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error in resharding process"})
		} else {
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			client.Do(req)
		}
	}

	//for loop update new shard KVs
	for idx, row := range newShardMembers {
		chunksToSend := chunkBody{
			ChunkInfo: chunks[idx],
		}
		kvJSON, err := json.Marshal(chunksToSend)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error in resharding process"})
		}
		for _, rep := range row {
			req, err := http.NewRequest(http.MethodPut, "http://"+rep+"/key-value-store-shard/chunk-r", bytes.NewBuffer(kvJSON))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Error in resharding process"})
			} else {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
				client.Do(req)
			}
		}
	}

	//if successful and nothing has failed

	c.JSON(http.StatusOK, gin.H{"message": "Resharding done successfully"})
}
