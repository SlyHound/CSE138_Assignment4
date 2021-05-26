package utility

import (
	"encoding/json"
	"io/ioutil"

	"github.com/gin-gonic/gin"
)

type ShardMsg struct {
	newShardCount int `json:shard-count`
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
		if len(shards.ShardOneMembers) < 2 {
			//reshard to put a server on shard one, if (<shards.MinCount) to reshard from shard two, send an error
		} else if len(shards.ShardTwoMembers) < 2 {
			//reshard to put a server on shard two, if (<shards.MinCount) to reshard from shard one, send an error
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

//Actual resharding algorithm
func reshard(view *View, personalSocketAddr string, shards *SharedShardInfo) {
	//Basic strategy is brute force
	//hash IP address, mod N, place in there
	//first GET preReshard state (store current shard members/view)
	for i := 0; i < len(view.PersonalView); i++ {
		//for each node, hash IP and then mod amount of shards given by our /reshard call
		//GET current
	}

}
