package utility

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type ShardMsg struct {
	NewShardCount int `json:"shard-count"`
}

type ShardIdResponse struct {
	ShardId int `json:"shard-id"`
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
		Mu.Mutex.Lock()
		startViewSize := len(view.PersonalView) //current length of view to see if new server is added
		Mu.Mutex.Unlock()
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

		//If we have a new shard count, RESHARD
		if ns.NewShardCount != shards.ShardCount {
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

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

//Actual resharding algorithm
func reshard(view *View, personalSocketAddr string, shards *SharedShardInfo, c *gin.Context) {
	//Basic strategy is brute force
	//hash IP address, mod N, place in there
	//first GET preReshard state (store current shard members/view)
	newShardMembers := [][]string{}
	var shardResp ShardIdResponse
	//First for loop builds newShardMembers
	for i := 0; i < len(view.PersonalView); i++ {
		currReplica := view.PersonalView[i]
		//for each node, hash IP and then mod amount of shards given by our /reshard call
		//GET the current shard ID of each node
		getNodeShardID, err := http.NewRequest("GET", "http://"+currReplica+"/key-value-store-shard/node-shard-id", nil)
		if err != nil {
			println("AHH ERROR IN RESHARDING")
		}
		httpForwarder := &http.Client{Timeout: 5 * time.Second}
		response, err := httpForwarder.Do(getNodeShardID)
		if err != nil {
			println("AHH ERROR IN RESHARDING")
		}
		defer response.Body.Close()
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			println("AHH ERROR IN RESHARDING")
		}
		json.Unmarshal(body, &shardResp)
		oldShardID := shardResp.ShardId
		newShardID := Hash(currReplica) % uint32(shards.ShardCount)
		if newShardID != uint32(oldShardID) {
			//call KvGet and replace local KvStore with that of replica in newShardID
		}
		newShardMembers[newShardID] = append(newShardMembers[newShardID], currReplica)

	}

	//second for loop, broadcasts newShardMembers to every replica in the view
	for i := 0; i < len(view.PersonalView); i++ {

	}

}
