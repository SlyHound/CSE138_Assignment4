package utility

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
)

type ShardMsg struct {
	newShardCount int `json:shard-count`
}

type chunkBody struct {
	chunkInfo map[string]StoreVal `json:chunk-info`
}
type newShards struct {
	newShards [][]string `json:shard-members`
}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

//Route to determine resharding request parameters
func ReshardRoute(view *View, personalSocketAddr string, shards *SharedShardInfo) {
	var ns ShardMsg //newShard request
	shards.Router.PUT("/key-value-store-shard/reshard", func(c *gin.Context) {
		startViewSize := len(view.PersonalView) //current length of view to see if new server is added
		body, _ := ioutil.ReadAll(c.Request.Body)
		json.Unmarshal(body, &ns)
		//Check if shard request has something in it, otherwise error
		defer c.Request.Body.Close()

		//If one shard has <2 members, we have to reshard
		for _, shard := range shards.ShardMembers {
			if len(shard) < 2 {
				//reshard
			}
		}
		if len(shards.ShardMembers) < 2 {
			//reshard to put a server on shard one, if (<shards.MinCount) to reshard from shard two, send an error
		}

		if len(view.PersonalView)/shards.ShardCount < 2 {

		}

		//If we have a new shard count, RESHARD
		if ns.newShardCount != shards.ShardCount {
			// Make sure we can have len(view.personalView)/ns.newShardCount >= 2
			// Otherwise return error
		}

		//If we have a new server added/dropped to the view, then we have to rebalance/redistribute
		newViewSize := len(view.PersonalView)
		if newViewSize < startViewSize {
			//reshard and rebalance the servers, send the server to the shard with the least amt of servers,
			//and in case of a tie, send to the first shard in our shard list
		} else if newViewSize > startViewSize {
			//if the removal reduced count < 2, then we'd already know from our case above
			//Otherwise, check the key discrepancy here, if the shard that just lost a server
		}
	})

}

//Route for chunk replication for new shard data
func ChunkRoute(view *View, personalSocketAddr string, s *SharedShardInfo) {
	var b chunkBody
	s.Router.PUT("/key-value-store-shard/chunk-r", func(c *gin.Context) {
		//Add everything sent in request to local storage
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}
		json.Unmarshal(body, &b)
		//replace whole chunk
		s.LocalKVStore = b.chunkInfo

		c.JSON(http.StatusOK, gin.H{})
	})
}

//Route for updating shard members
func UpdateShardMembers(s *SharedShardInfo) {
	var d newShards
	s.Router.PUT("/key-value-store-shard/updatesm", func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}
		json.Unmarshal(body, &d)
		s.ShardMembers = d.newShards

		c.JSON(http.StatusOK, gin.H{})
	})

}

//function to determine what shard we should place k-v pair in
func hashModN(s string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() % uint32(n))
}

//Actual resharding algorithm
//LOCK THIS WHOLE FUNCTION
func reshard(view *View, personalSocketAddr string, shards *SharedShardInfo, c *gin.Context) {
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
	toBeSharded := []string{}
	copy(toBeSharded, view.PersonalView)
	evenSplit := len(view.PersonalView) / shards.ShardCount //keeps general count for how many replicas we should have in each shard
	currShardID := 0
	// while we have replicas to "shard"
	for len(toBeSharded) > 0 {
		//move to next shard if we have at least an even split in our current one, otherwise add to currentShard
		if len(newShardMembers[currShardID]) >= evenSplit {
			currShardID++
		} else if currShardID > shards.ShardCount-1 {
			//If we have already done an even split and there's a remainder of our replicas, just append to last shard and then make array
			newShardMembers[currShardID] = append(newShardMembers[currShardID-1], toBeSharded...)
			toBeSharded = nil //or break
		} else {
			newShardMembers[currShardID] = append(newShardMembers[currShardID], toBeSharded[0])
			toBeSharded = toBeSharded[1:]
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
			newShardID := hashModN(key, shards.ShardCount)
			chunks[newShardID][key] = value
			if newShardID != oldShardID {
				//Delete from the original shardKV
				delete(shardKV, key)
			}
		}

	}

	//broadcast new shards out to every replica
	shards.ShardMembers = newShardMembers
	//second for loop, broadcasts newShardMembers to every replica in the view
	updatedShards := newShards{
		newShards: newShardMembers,
	}
	client := &http.Client{}
	usJSON, err := json.Marshal(updatedShards)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{})
	}
	for i := 0; i < len(view.PersonalView); i++ {
		req, err := http.NewRequest(http.MethodPut, "http://"+view.PersonalView[i]+"/key-value-store-shard/updatesm", bytes.newBuffer(usJSON))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
		} else {
			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			client.Do(req)
		}
	}

	//for loop update new shard KVs
	for idx, row := range newShardMembers {
		chunksToSend := chunkBody{
			chunkInfo: chunks[idx],
		}
		kvJSON, err := json.Marshal(chunksToSend)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{})
		}
		for _, rep := range row {
			req, err := http.NewRequest(http.MethodPut, "http://"+rep+"/key-value-store-shard/chunk-r", bytes.newBuffer(kvJSON))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{})
			} else {
				req.Header.Set("Content-Type", "application/json; charset=utf-8")
				client.Do(req)
			}
		}
	}
}
