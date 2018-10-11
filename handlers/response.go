package handlers

import "github.com/docker/docker/daemon/logger"

import (
	"encoding/json"
	"net/http"
)

type CapabilitiesResponse struct {
	Err string
	Cap logger.Capability
}

type response struct {
	Err string
}

func respond(err error, w http.ResponseWriter) {

	var res response
	if err != nil {
		res.Err = err.Error()
	}
	json.NewEncoder(w).Encode(&res)
}
