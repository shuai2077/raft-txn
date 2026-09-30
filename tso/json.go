package tso

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
)

// decodeJSON decodes a request body, requiring an application/json content type
// and rejecting unknown fields so client and server cannot drift apart.
func decodeJSON(req *http.Request, target any) error {
	mediaType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	if mediaType != "application/json" {
		return fmt.Errorf("expect application/json Content-Type, got %q", mediaType)
	}
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

// writeJSON renders v as JSON.
func writeJSON(w http.ResponseWriter, v any) {
	js, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(js)
}
