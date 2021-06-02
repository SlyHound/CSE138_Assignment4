package utility

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

type SockAddr struct {
	Address string `json:"socket-address"`
}

type Mutex struct {
	Mutex sync.Mutex
}

var (
	Mu Mutex
)

/* this function deletes the replica from its own
view and the replica from all other replica's views */
func RequestDelete(v *View, personalSocketAddr string, indiciesToRemove map[int]string) {
	Mu.Mutex.Lock()
	for index, addr := range v.PersonalView {
		if addr == personalSocketAddr { // skip over the personal replica since we don't send to ourselves
			continue
		} else if indiciesToRemove[index] == addr { // if the element exists in the map, then delete it //

			copiedViewElem := v.PersonalView[index]
			data := strings.NewReader(`{"socket-address":"` + copiedViewElem + `"}`)
			request, err := http.NewRequest("DELETE", "http://"+copiedViewElem+"/key-value-store-view", data)

			if err != nil {
				log.Fatal("There was an error creating a DELETE request with error:", err.Error())
			}
			Mu.Mutex.Unlock()
			request.Header.Set("Content-Type", "application/json")
			httpForwarder := &http.Client{}
			response, err := httpForwarder.Do(request)
			Mu.Mutex.Lock()

			if err != nil { // if a response doesn't come back, then that replica might be down
				fmt.Println("There was an error sending a DELETE request to " + v.PersonalView[index])
				continue
			}
			defer response.Body.Close()
		}
	}
	Mu.Mutex.Unlock()

	var allKeys []int

	// gets all the keys first to sort before removing all the replica's that failed to get a rqst back //
	for index := range indiciesToRemove {
		allKeys = append(allKeys, index)
	}
	// fmt.Println("Check allKeys:", allKeys)

	sort.Sort(sort.Reverse(sort.IntSlice(allKeys)))

	Mu.Mutex.Lock()
	for _, index := range allKeys {
		if indiciesToRemove[index] == v.PersonalView[index] { // if the replica hasn't been removed yet, then remove it
			if index+1 < len(v.PersonalView) {
				v.PersonalView = append(v.PersonalView[:index], v.PersonalView[index+1:]...)
			} else {
				v.PersonalView = v.PersonalView[:len(v.PersonalView)-1]
			}
		}
	}
	Mu.Mutex.Unlock()

	// v = DeleteDuplicates(v)
	// fmt.Println("Check allSocketAddrs in rqstDelete:", v)
}

func ResponseDelete(r *gin.Engine, view *View) {
	var d SockAddr
	r.DELETE("/key-value-store-view", func(c *gin.Context) {
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			fmt.Println("There was an error attempting to read the request body.")
			c.JSON(http.StatusInternalServerError, gin.H{})
		}

		strBody := string(body[:])
		// fmt.Println("Check strBody in respDelete:", strBody)
		json.NewDecoder(strings.NewReader(strBody)).Decode(&d)
		// fmt.Println("Check d.SocketAddr in respDelete:", d.Address)
		defer c.Request.Body.Close()

		presentInView := false
		oIndex := 0

		for index, viewSocketAddr := range view.PersonalView {
			if d.Address == viewSocketAddr {
				presentInView = true
				oIndex = index
				break
			}
		}

		// if the passed in socket address is present in the current replica's view, then delete it, else 404 error //
		if presentInView {
			Mu.Mutex.Lock()
			view.PersonalView = append(view.PersonalView[:oIndex], view.PersonalView[oIndex+1:]...) // deletes the replica from the current view that received the DELETE rqst. //
			Mu.Mutex.Unlock()
			// fmt.Println("Check view in respDelete:", view)
			c.JSON(http.StatusOK, gin.H{"message": "Replica deleted successfully from the view"})
		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "Socket address does not exist in the view", "message": "Error in DELETE"})
		}
	})
}
