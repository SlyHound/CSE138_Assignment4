package utility

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type allMembers struct {
	receivedShardMembers [][]string // member of all shards that is received from the node that is broadcasting
}

// type receivedMembers struct {
// 	Message string     `json:"message"`
// 	Members [][]string `json:"shard-id-members"` // members received from /shard-id-members endpoint
// }

// adds a new node to a given shard
func AddNode(v *View, s *SharedShardInfo) {
	var (
		d Body // Body struct defined in viewPut.go //
	)
	s.Router.PUT("/key-value-store-shard/add-member/:id", func(c *gin.Context) {
		shardId := c.Param("id")
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}

		json.Unmarshal(body, &d)
		intShardId, _ := strconv.Atoi(shardId)

		// first we need to get the members of our shard, so broadcast a GET request to all shards (only need one response back) //
		// Mu.Mutex.Lock()
		// for index := range v.PersonalView {
		// 	request, err := http.NewRequest("GET", "http://"+v.PersonalView[index]+"/key-value-store-shard/shard-id-all_members/"+shardId, nil)

		// 	if err != nil {
		// 		Mu.Mutex.Unlock()
		// 		log.Fatal("error attempting to create a new GET request:", err.Error())
		// 	}

		// 	Mu.Mutex.Unlock()
		// 	httpForwarder := &http.Client{} // alias for DefaultClient
		// 	response, err := httpForwarder.Do(request)
		// 	Mu.Mutex.Lock()

		// 	// the node could be down when attempting to send to it, so we continue to attempt to send to the other nodes //
		// 	if err != nil {
		// 		continue
		// 	}
		// 	defer response.Body.Close()
		// 	body, _ := ioutil.ReadAll(response.Body)
		// 	json.Unmarshal(body, &m)

		// 	s.ShardMembers = make([][]string, len(m.Members))

		// 	for index := range m.Members {
		// 		s.ShardMembers[index] = append(s.ShardMembers[index], m.Members[index]...)
		// 	}
		// 	fmt.Printf("****s.ShardMembers IN shardPut.go: %v*******", s.ShardMembers)
		// 	s.CurrentShard = GetCurrentShardId(s, v.SocketAddr)
		// 	break // once we retrieve the shard members successfully, we can stop broadcasting
		// }
		// Mu.Mutex.Unlock()

		index := 0
		Mu.Mutex.Lock()
		for shardIndex := range s.ShardMembers {
			if intShardId == shardIndex {
				index = shardIndex
				break
			}
		}

		s.ShardMembers[index] = append(s.ShardMembers[index], d.Address)
		Mu.Mutex.Unlock()

		// broadcast to the members in the current shard the new shard member //
		Mu.Mutex.Lock()
		for index := range v.PersonalView {
			if v.PersonalView[index] == v.SocketAddr {
				continue
			}
			body, _ := json.Marshal(s.ShardMembers)
			request, err := http.NewRequest("PUT", "http://"+v.PersonalView[index]+"/key-value-store-shard/use-members", bytes.NewBuffer(body))

			if err != nil {
				Mu.Mutex.Unlock()
				log.Fatal("error attempting to create a new PUT request:", err.Error())
			}
			Mu.Mutex.Unlock()
			httpForwarder := &http.Client{} // alias for DefaultClient
			response, err := httpForwarder.Do(request)
			Mu.Mutex.Lock()

			// the node could be down when attempting to send to it, so we continue to attempt to send to the other nodes //
			if err != nil {
				continue
			}
			defer response.Body.Close()
		}
		Mu.Mutex.Unlock()

		c.JSON(http.StatusOK, gin.H{}) // sends back just the 200 status code with no message body
	})
}

// helper function that uses the currently received shard members slice of slices
func NewShardMember(s *SharedShardInfo, view *View) {
	var am allMembers
	s.Router.PUT("/key-value-store-shard/use-members", func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body) // need to figure out how to parse a slice of slices

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}

		strBody := string(body[:])
		json.NewDecoder(strings.NewReader(strBody)).Decode(&am.receivedShardMembers)

		// set the current shardMembers slice of slices to be that of received shardMembers
		Mu.Mutex.Lock()
		if len(s.ShardMembers) == 0 {
			s.ShardMembers = make([][]string, len(am.receivedShardMembers))
		}
		for shardIndex, shardMembers := range am.receivedShardMembers {
			s.ShardMembers[shardIndex] = append(s.ShardMembers[shardIndex], shardMembers...)
		}
		Mu.Mutex.Unlock()
		fmt.Printf("****s.ShardMembers IN shardPut.go: %v*******", s.ShardMembers)
	})
}
