package utility

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type getView struct {
	Message string
	View    []string
}

type getData struct {
	Message string
	Store   map[string]StoreVal
}

type View struct {
	PersonalView []string // contains all nodes that are currently up
	NewReplica   string   // pertains only to PUT requests
	SocketAddr   string   // the current node's socket address
}

/* this function will broadcast a GET request from one replica to all other
   replica's to ensure that they are currently up. */
func RequestGet(v *View, personalSocketAddr string) ([]string, map[int]string) {
	var (
		g getView
	)
	noResponseIndices := make(map[int]string)

	Mu.Mutex.Lock()
	// fmt.Println("Check v.PersonalView before for in RqstGet:", v.PersonalView)
	for index, addr := range v.PersonalView {
		if addr == personalSocketAddr { // skip over the personal replica since we don't send to ourselves
			continue
		}
		fmt.Println("allSocketAddrs[index], index:", v.PersonalView[index], index)
		request, err := http.NewRequest("GET", "http://"+v.PersonalView[index]+"/key-value-store-view", nil)
		if err != nil {
			log.Fatal("There was an error creating a GET request with the following error:", err.Error())
		}

		Mu.Mutex.Unlock()
		// try to send a GET request 5 times //
		var response *http.Response
		for i := 0; i < 5; i++ {
			httpForwarder := &http.Client{Timeout: 3 * time.Second} // alias for DefaultClient
			response, err = httpForwarder.Do(request)
			if err != nil {
				time.Sleep(time.Second * 3)
				fmt.Printf("ATTEMPTING TO SEND %v MORE TIMES & check err: %v", i, err.Error())
			} else {
				break
			}
		}
		Mu.Mutex.Lock()
		fmt.Println("Check personalView length in viewGet.go: ", len(v.PersonalView))
		if err != nil { // if a response doesn't come back, then that replica might be down
			fmt.Println("There was an error sending a GET request to " + v.PersonalView[index])
			noResponseIndices[index] = v.PersonalView[index]
			continue
		}
		body, _ := ioutil.ReadAll(response.Body)
		strBody := string(body[:])
		json.NewDecoder(strings.NewReader(strBody)).Decode(&g)
		response.Body.Close()
	}
	Mu.Mutex.Unlock()

	return g.View, noResponseIndices
}

// returns the key value store of another replica's or an empty map if the request fell through
func KvGet(replica string) map[string]StoreVal {
	var getData getData
	request, err := http.NewRequest("GET", "http://"+replica+"/key-value-store-values", nil)

	if err != nil {
		log.Fatal("There was an error creating a GET request with the following error:", err.Error())
	}

	httpForwarder := &http.Client{} // alias for DefaultClient
	response, err := httpForwarder.Do(request)

	if err != nil { // if a response doesn't come back, then that replica might be down
		fmt.Println("There was an error sending a GET request to " + replica)
		return nil
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	strBody := string(body[:])

	if err != nil {
		log.Fatal("There was an error reading all the bytes from the response body, where the following error is present:", err.Error())
	}

	json.NewDecoder(strings.NewReader(strBody)).Decode(&getData)
	return getData.Store
}

func ResponseGet(r *gin.Engine, view *View) {
	r.GET("/key-value-store-view", func(c *gin.Context) {
		fmt.Println("GET rqst. received")
		// Mu.Mutex.Lock()
		c.JSON(http.StatusOK, gin.H{"message": "View retrieved successfully", "view": view.PersonalView})
		// Mu.Mutex.Unlock()
	})
}

// custom function designed to get all key-value pairs of the current replica to store in the new replica's store //
func KeyValueResponse(r *gin.Engine, store map[string]StoreVal) {
	r.GET("/key-value-store-values", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "All pairs retrieved successfully", "store": store})
	})
}
