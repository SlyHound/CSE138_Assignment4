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
					if !inReplica {
						view.NewReplica = viewSocketAddr
						break
					}
				}
			}
		}
		utility.Mu.Mutex.Unlock()

		if view.NewReplica != "" { // broadcast a PUT request with the new replica to add to all replica's views
			// fmt.Println("Before rqstPut call")
			utility.RequestPut(view, personalSocketAddr)
			// fmt.Println("Check view in healthCheck after PUT:", view)
			utility.Mu.Mutex.Lock()
			if len(kvStore) == 0 { // if the current key-value store is empty, then we need to retrieve k-v pairs from the other replica's in the current shard
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
}

func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func setupRouter(kvStore map[string]utility.StoreVal, socketAddr string, view []string, currVC []int) *gin.Engine {
	router := gin.Default()
	gin.SetMode(gin.ReleaseMode)
	// keep global variable of our SOCKET ADDRESS
	gin.DefaultWriter = ioutil.Discard
	var socketIdx int
	fmt.Printf("%v\n", view)
	for i := 0; i < len(view); i++ {
		println(view[i])
		if view[i] == socketAddr {
			// println("VIEW[i]: " + view[i])
			// println("SOCKETADDR: " + socketAddr)
			socketIdx = i
			//set VCIndex to i
			//funky stuff here, may be unneeded, don't remove for now
			if i == 0 {
				view = view[1:]
			} else {
				view = remove(view, i)
			}
		}
	}
	fmt.Printf("%v\n", view)

	// main functionality from assignment 2, basically need to modify the PUTS and DELETES to echo to other
	utility.PutRequest(router, kvStore, socketIdx, view, currVC)
	utility.GetRequest(router, kvStore, socketIdx, view)
	utility.DeleteRequest(router, kvStore, socketIdx, view, currVC)
	utility.ReplicatePut(router, kvStore, socketIdx, view, currVC)
	utility.ReplicateDelete(router, kvStore, socketIdx, view, currVC)
	return router
}

func main() {
	var kvStore = make(map[string]utility.StoreVal) // key-value store for PUT, GET, & DELETE requests (exported variable)

	socketAddr := os.Getenv("SOCKET_ADDRESS")
	view := strings.Split(os.Getenv("VIEW"), ",")
	shardCount := os.Getenv("SHARD_COUNT")

	currVC := []int{0, 0, 0, 0}

	v := &utility.View{}
	v.PersonalView = append(v.PersonalView, view...)
	v.NewReplica = ""

	shards := &utility.SharedShardInfo{}
	intShardCount, _ := strconv.Atoi(shardCount)
	shards.ShardMembers = make([][]string, intShardCount)
	shards.CurrentShard = 0 // set it to 0 for now, just for initialization purposes
	utility.InitialSharding(shards, v, shardCount)

	go healthCheck(v, socketAddr, kvStore, shards)

	router := setupRouter(kvStore, socketAddr, view, currVC)
	shards.Router = router
	variousResponses(kvStore, v, shards)

	err := router.Run(port)

	if err != nil {
		fmt.Println("There was an error attempting to run the router on port", port, "with the error", err)
	}
}
