package utility

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type body struct {
	Address string `json:"socket-address"`
}

func RequestPut(v *View, personalSocketAddr string) {

	Mu.Mutex.Lock()
	// now broadcast a PUT request to all other replica's to add it to their view's //
	data, err := json.Marshal(body{Address: v.NewReplica})

	if err != nil {
		log.Fatal("There was an error marshalling data.")
	}

	for index, addr := range v.PersonalView {
		if addr == personalSocketAddr { // skip over the personal replica since we don't send to ourselves
			continue
		}
		copiedViewElem := v.PersonalView[index]
		request, err := http.NewRequest("PUT", "http://"+copiedViewElem+"/key-value-store-view", bytes.NewReader(data))

		if err != nil {
			log.Fatal("There was an error creating a PUT request.")
		}

		// fmt.Println("Sending to ", v.PersonalView[index], "with data about v.NewReplica:", v.NewReplica)

		request.Header.Set("Content-Type", "application/json")
		httpForwarder := &http.Client{}
		response, err := httpForwarder.Do(request)

		if err != nil { // if a response doesn't come back, then that replica might be down
			fmt.Println("There was an error sending a PUT request to " + v.PersonalView[index])
			continue
		}
		defer response.Body.Close()
	}

	// can remove lines 46 - 57 I think, since only the wokem replica sends put requests, so no point in adding to myself //
	addedAlready := false
	for index := range v.PersonalView {
		if v.PersonalView[index] == v.NewReplica {
			addedAlready = true
			break
		}
	}

	// add the new replica to the current view if it hasn't already been added //
	if !addedAlready {
		v.PersonalView = append(v.PersonalView, v.NewReplica)
	}

	Mu.Mutex.Unlock()
}

func ResponsePut(r *gin.Engine, view *View) {
	var (
		d SockAddr
	)

	r.PUT("/key-value-store-view", func(c *gin.Context) {
		// fmt.Println("Check c.Request:", c.Request)
		body, err := ioutil.ReadAll(c.Request.Body)

		if err != nil {
			fmt.Println("There was an error attempting to read the request body.")
			c.JSON(http.StatusInternalServerError, gin.H{})
		}

		strBody := string(body[:])
		// fmt.Println("Check strBody in RespPut:", strBody)
		json.NewDecoder(strings.NewReader(strBody)).Decode(&d)
		Mu.Mutex.Lock()

		addedAlready := false
		for index := range view.PersonalView {
			if view.PersonalView[index] == d.Address {
				addedAlready = true
				break
			}
		}

		// fmt.Println("Check d.Address & view.PersonalView in ResponsePut:", d.Address, view.PersonalView)

		if !addedAlready {
			view.PersonalView = append(view.PersonalView, d.Address) // adds the new replica to the view //
		}
		// fmt.Println("Check view.PersonalView after appending d.Address:", view.PersonalView)

		Mu.Mutex.Unlock()
		c.Request.Body.Close()

		if addedAlready {
			c.JSON(http.StatusNotFound, gin.H{"error": "Socket address already exists in the view", "message": "Error in PUT"})
		} else {
			c.JSON(http.StatusCreated, gin.H{"message": "Replica added successfully to the view"})
		}
	})
}
