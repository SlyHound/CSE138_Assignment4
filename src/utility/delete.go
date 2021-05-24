package utility

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Metadata struct {
	CausalMetadata []int `json:"causal-metadata"`
}

//DeleteRequest Client endpoint for deletions
func DeleteRequest(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int) {
	var m Metadata
	println(view)
	r.DELETE("/key-value-store/:key", func(c *gin.Context) {
		key := c.Param("key")

		// if the key-value pair exists, then delete it //
		if _, exists := dict[key]; exists {
			if len(m.CausalMetadata) > 0 {
				updateKvStore(view, dict, currVC)
			} else if len(m.CausalMetadata) == 0 {
				m.CausalMetadata = []int{0, 0, 0}
			}
			// increment on receive so we send back correct causal clock
			m.CausalMetadata[localAddr]++
			m.CausalMetadata = append(m.CausalMetadata, localAddr) //Index of sender address
			currVC = m.CausalMetadata
			c.JSON(http.StatusOK, gin.H{"message": "Deleted successfully", "causal-metadata": m.CausalMetadata[0:3]})
			delete(dict, key)
		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "Key does not exist", "message": "Error in DELETE"})
		}

		//Broadcast delete to all other replicas
		for i := 0; i < len(view); i++ {
			println("Replicating message to: " + "http://" + view[i] + "/key-value-store-r/" + key)
			c.Request.URL.Host = view[i]
			c.Request.URL.Scheme = "http"
			data := &Metadata{CausalMetadata: m.CausalMetadata}
			jsonData, _ := json.Marshal(data)
			fwdRequest, err := http.NewRequest("DELETE", "http://"+view[i]+"/key-value-store-r/"+key, bytes.NewBuffer(jsonData))
			if err != nil {
				http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
				return
			}

			fwdRequest.Header = c.Request.Header

			httpForwarder := &http.Client{}
			fwdResponse, err := httpForwarder.Do(fwdRequest)
			_ = fwdResponse
			// Shouldn't worry about Error checking? just send requests out and if things are down oh well?
			//TODO
			//USE THIS CATCH TO SEE IF SERVER IS DOWN AND UPDATE IN VIEW @Alex
			// if err != nil {
			// 	msg := "Error in " + fwdRequest.Method
			// 	c.JSON(http.StatusServiceUnavailable, gin.H{"error": view[i] + " is down", "message": msg})
			// }
			// if fwdResponse != nil {
			// 	//TODO
			// 	//Comment/delete to not chain requests back to client
			// 	body, _ := ioutil.ReadAll(fwdResponse.Body)
			// 	rawJSON := json.RawMessage(body)
			// 	c.JSON(fwdResponse.StatusCode, rawJSON)
			// 	defer fwdResponse.Body.Close()
			// }
		}
	})

}

//ReplicateDelete endpoint to replicate delete messages
func ReplicateDelete(r *gin.Engine, dict map[string]StoreVal, localAddr int, view []string, currVC []int) {
	var m Metadata
	r.DELETE("/key-value-store-r/:key", func(c *gin.Context) {
		key := c.Param("key")
		body, _ := ioutil.ReadAll(c.Request.Body)
		strBody := string(body[:])
		fmt.Printf("STRBODY: %s\n", strBody)
		json.Unmarshal(body, &m)
		fmt.Printf("CAUSAL CLOCK VALUE: %v\n", m.CausalMetadata)
		// if the key-value pair exists, then delete it //
		if _, exists := dict[key]; exists {
			if canDeliver(m.CausalMetadata, currVC) {
				m.CausalMetadata = dict[key].CausalMetadata
				c.JSON(http.StatusOK, gin.H{"message": "Deleted successfully", "causal-metadata": m.CausalMetadata})
				delete(dict, key)
			} else {
				updateKvStore(view, dict, currVC)
				m.CausalMetadata = updateVC(m.CausalMetadata, currVC)
				c.JSON(http.StatusOK, gin.H{"message": "Deleted successfully", "causal-metadata": m.CausalMetadata})
				delete(dict, key)
			}

		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "Key does not exist", "message": "Error in DELETE"})
		}
	})
}
