package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"src/utility"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	port = ":8085"
)

// checks to ensure that replica's are up by broadcasting GET requests //
func healthCheck(view *utility.View, personalSocketAddr string, kvStore map[string]utility.StoreVal, s *utility.SharedShardInfo) {

	// runs infinitely on a 1 second clock interval //
	interval := time.Tick(time.Second * 1)
	for range interval {
		/* If a request returns with a view having # of replicas > current view
		   then broadcast a PUT request (this means a replica has been added to the system) */
		returnedView, noResponseIndices := utility.RequestGet(view, personalSocketAddr)
		// fmt.Println("Check response received:", returnedView, noResponseIndices)

		/* call upon RequestDelete to delete the replica from its own view and
		   broadcast to other replica's to delete that same replica from their view */
		utility.RequestDelete(view, personalSocketAddr, noResponseIndices)

		fmt.Println("Check view & returnedView in healthCheck before for:", view, returnedView)
		inReplica := false

		utility.Mu.Mutex.Lock()
		if len(returnedView) > 0 {
			for _, viewSocketAddr := range view.PersonalView {
				inReplica = false
				for _, recvSocketAddr := range returnedView {
					if viewSocketAddr == recvSocketAddr {
						inReplica = true
						break
					}
				}
				if !inReplica {
					view.NewReplica = viewSocketAddr
					break
				}
			}
		}
		utility.Mu.Mutex.Unlock()

		if view.NewReplica != "" { // broadcast a PUT request with the new replica to add to all replica's views
			// fmt.Println("Before rqstPut call")
			utility.RequestPut(view, personalSocketAddr)
			// fmt.Println("Check view in healthCheck after PUT:", view)
			utility.Mu.Mutex.Lock()
			if len(kvStore) == 0 && len(s.ShardMembers) > 0 { // if the current key-value store is empty, then we need to retrieve k-v pairs from the other replica's in the current shard
				s.CurrentShard = utility.GetCurrentShardId(s, view.SocketAddr)
				for _, addr := range s.ShardMembers[s.CurrentShard] {
					if addr == personalSocketAddr {
						continue
					}
					dictValues := utility.KvGet(addr)
					fmt.Println("*********DICTVALUES ***********", dictValues)
					// updates the current replica's key-value store with that of the received key-value store
					for key, storeVal := range dictValues {
						_, exists := kvStore[key]
						if !exists { // if the key doesn't exist in the store, then add it
							kvStore[fmt.Sprint(key)] = utility.StoreVal{Value: storeVal.Value, CausalMetadata: storeVal.CausalMetadata}
						}
					}
				}
				// fmt.Println("Check GET response on values:", dictValues)
			}
			utility.Mu.Mutex.Unlock()
		}
	}
}

func variousResponses(store map[string]utility.StoreVal, view *utility.View, s *utility.SharedShardInfo) {
	utility.ResponseGet(s.Router, view)
	utility.ResponseDelete(s.Router, view)
	utility.ResponsePut(s.Router, view)
	utility.KeyValueResponse(s.Router, store)
	utility.GetAllShardIds(s)
	utility.GetMembers(s)
	utility.GetNodeShardId(s)
	utility.GetNumKeys(store, s)
	utility.AddNode(view, s)
	utility.NewShardMember(s, view)
}

func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func setupRouter(kvStore map[string]utility.StoreVal, view []string, v *utility.View, currVC []int, shard *utility.SharedShardInfo) *gin.Engine {
	router := gin.Default()
	shard.Router = router
	gin.SetMode(gin.ReleaseMode)
	// keep global variable of our SOCKET ADDRESS
	gin.DefaultWriter = ioutil.Discard
	var socketIdx int
	fmt.Printf("%v\n", view)
	for i := 0; i < len(view); i++ {
		if view[i] == v.SocketAddr {
			socketIdx = i
			if i == 0 {
				view = view[1:]
			} else {
				view = remove(view, i)
			}
		}
	}

	// utility.GetRequest(router, kvStore, socketIdx, view)
	utility.ShardPutStore(shard, v, kvStore, socketIdx, currVC)
	utility.ShardGetStore(shard, v, kvStore, socketIdx, currVC)
	utility.DeleteRequest(router, kvStore, socketIdx, v.PersonalView, currVC, shard)
	utility.ReplicatePut(router, kvStore, socketIdx, v.PersonalView, currVC, shard)
	utility.ReplicateDelete(router, kvStore, socketIdx, v.PersonalView, currVC, shard)
	return router
}

func main() {
	var kvStore = make(map[string]utility.StoreVal) // key-value store for PUT, GET, & DELETE requests (exported variable)

	socketAddr := os.Getenv("SOCKET_ADDRESS")
	view := strings.Split(os.Getenv("VIEW"), ",")
	shardCount := os.Getenv("SHARD_COUNT")

	currVC := make([]int, len(view))
	for range view {
		currVC = append(currVC, 0)
	}
	// currVC := []int{0, 0, 0, 0}

	v := &utility.View{}
	v.PersonalView = append(v.PersonalView, view...)
	v.NewReplica = ""
	v.SocketAddr = socketAddr

	shards := &utility.SharedShardInfo{}
	intShardCount, _ := strconv.Atoi(shardCount)

	// we must first initialize the properties of the struct, before using it
	shards.ShardMembers = make([][]string, intShardCount)
	shards.CurrentShard = 0 // init value
	shards.ShardCount, _ = strconv.Atoi(shardCount)
	shards.MinNodes = 2 //default value
	shards.Router = nil // initialized value (not actually used)

	router := setupRouter(kvStore, view, v, currVC, shards)

	if shardCount != "" {
		utility.InitialSharding(shards, v, shardCount)
		shards.CurrentShard = utility.GetCurrentShardId(shards, socketAddr)
	}

	go healthCheck(v, socketAddr, kvStore, shards)

	variousResponses(kvStore, v, shards)

	err := router.Run(port)

	if err != nil {
		fmt.Println("There was an error attempting to run the router on port", port, "with the error", err)
	}
}