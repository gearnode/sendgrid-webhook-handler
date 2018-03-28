package main

import (
	"encoding/json"
	"encoding/base64"
	"net/http"
	"log"
	"time"
	"strings"
)

const(
	USERNAME = "test"
	PASSWORD = "test"
)

func l(msg string) string {
	x := map[string]string{
		"message": msg,
		"ts": time.Now().UTC().String(),
	}
	payload, _ := json.Marshal(x)
	return string(payload)
}


func handleEvent(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		log.Println(l("405 " + r.Method + " " + r.URL.String()))
		resp := map[string]string{"error": "status_method_not_allowed"}
		msg, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(msg))
		return
	}

	auth := strings.SplitN(r.Header.Get("Authorization"), " ", 2)

	if len(auth) != 2 || auth[0] != "Basic" {
		log.Println(l("401 " + r.Method + " " + r.URL.String()))
		resp := map[string]string{"error": "authorization_method_not_allowed"}
		msg, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(msg))
		return
	}

	payload, _ := base64.StdEncoding.DecodeString(auth[1])
	pair := strings.SplitN(string(payload), ":", 2)

	if len(pair) != 2 || pair[0] != USERNAME || pair[1] != PASSWORD {
		log.Println(l("401 " + r.Method + " " + r.URL.String()))
		resp := map[string]string{"error": "authorization_failed"}
		msg, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(msg))
		return
	}

	var payload map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		// Handle decoding error
		log.Println(err)
		return
	}

	log.Println(l("200 " + r.Method + " " + r.URL.String()))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("{}"))
}

func handleNotFound(w http.ResponseWriter, r *http.Request) {
	resp := map[string]string{"error": "not_found"}

	log.Println(l("404 " + r.Method + " " + r.URL.String()))

	msg, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(msg))
}

func main() {
	log.Println(l("start server :3000"))
	http.HandleFunc("/callback", handleEvent)
	http.HandleFunc("/", handleNotFound)
	log.Fatal(http.ListenAndServe(":3000", nil))
}