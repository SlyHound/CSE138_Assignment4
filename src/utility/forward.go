package utility

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gin-gonic/gin"
)

type JSON struct {
	Error    string `json:"error"`
	Response string `json:"message"`
}

func ForwardRequest(r *gin.Engine, view string) {
	fmt.Println("In ForwardRequest call")
	r.PUT("/key-value-store/:key", handleRequests(view))
	r.DELETE("/key-value-store/:key", handleRequests(view))
}

func handleRequests(view string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.URL.Host = view
		c.Request.URL.Scheme = "http"

		fwdRequest, err := http.NewRequest(c.Request.Method, c.Request.URL.String(), c.Request.Body)
		if err != nil {
			http.Error(c.Writer, err.Error(), http.StatusInternalServerError)
			return
		}

		fwdRequest.Header = c.Request.Header

		httpForwarder := &http.Client{}
		fwdResponse, err := httpForwarder.Do(fwdRequest)

		// Shouldn't worry about Error checking? just send requests out and if things are down oh well?
		if err != nil {
			msg := "Error in " + fwdRequest.Method
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Main instance is down", "message": msg})
		}
		if fwdResponse != nil {
			body, _ := ioutil.ReadAll(fwdResponse.Body)
			rawJSON := json.RawMessage(body)
			c.JSON(fwdResponse.StatusCode, rawJSON)
			defer fwdResponse.Body.Close()
		}
	}
}
