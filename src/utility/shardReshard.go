package utility

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type ShardMsg struct {
	newShardCount int `json:shard-count`
}

type shardIdResponse struct {
	shardId int
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

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

//Actual resharding algorithm
func reshard(view *View, personalSocketAddr string, shards *SharedShardInfo, c *gin.Context) {
	//Basic strategy is brute force
	//hash IP address, mod N, place in there
	//first GET preReshard state (store current shard members/view)
	for i := 0; i < len(view.PersonalView); i++ {
		//for each node, hash IP and then mod amount of shards given by our /reshard call
		//GET the current shard ID of each node
		fwdRequest, err := http.NewRequest("GET", "http://"+view.PersonalView[i]+"/key-value-store-shard/node-shard-id", nil)
		if err != nil {
			http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
			return
		}
		fwdRequest.Header = c.Request.Header
		httpForwarder := &http.Client{Timeout: 5 * time.Second}
		//Actual send request here
		fwdResponse, err := httpForwarder.Do(fwdRequest)
		_ = fwdResponse

		//Find new shard for the node
		//We determine our server shard the same way we determine where out keys go to, we do hash(key) % numShards
		//where the key is the IP address in this case
		newNodeID := int(hash(view.PersonalView[i]) % uint32(shards.ShardCount))

		//Remove node from old shard, and add to new shard
		//TODO: do we need to re-send data from the other nodes in shard to replicate to new node? or will that be done automatically?
		//Do we need to broadcast this request? or just send it once?
		newShardReq, err := http.NewRequest("PUT", "http://"+view.PersonalView[i]+"/key-value-store-shard/add-member/"+strconv.Itoa(newNodeID), nil)
		newShardResp, err := httpForwarder.Do(newShardReq)
		println(newShardResp)
	}

}
