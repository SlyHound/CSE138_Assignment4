package utility

import (
	"bytes"
	"encoding/json"
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

// adds a new node to a given shard
func AddNode(v *View, s *SharedShardInfo) {
	var d Body // Body struct defined in viewPut.go //
	s.Router.PUT("/key-value-store-shard/add-member/:id", func(c *gin.Context) {
		shardId := c.Param("id")
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Issue reading bytes from the request body"})
		}

		json.Unmarshal(body, &d)
		intShardId, _ := strconv.Atoi(shardId)

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

		// broadcast to the new shard members slice of slices to all other nodes //
		Mu.Mutex.Lock()
		for index := range v.PersonalView {
			if v.PersonalView[index] == v.SocketAddr {
				continue
			}
			body, _ := json.Marshal(s.ShardMembers)
			request, err := http.NewRequest("PUT", "http://"+v.PersonalView[index]+"key-value-store-shard/use-members", bytes.NewBuffer(body))

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
func NewShardMember(s *SharedShardInfo) {
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
		s.ShardMembers = am.receivedShardMembers
		Mu.Mutex.Unlock()
	})
}
