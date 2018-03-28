package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	USERNAME = "test"
	PASSWORD = "test"
)

func l(msg string) string {
	x := map[string]string{
		"message": msg,
		"ts":      time.Now().UTC().String(),
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

	parse, _ := base64.StdEncoding.DecodeString(auth[1])
	pair := strings.SplitN(string(parse), ":", 2)

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

	bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		panic(errors.New("BOOTSTRAP_SERVERS environment variable is nil."))
	}
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	// Monitor message delivery
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Println(l("Delivery success"))
				} else {
					log.Println(l("Delivery failed"))
				}
			}
		}
	}()

	// loop to produce split array in sigle event
	topic := "sendgrid"
	y, _ := json.Marshal(payload)
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          y,
	}, nil)

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
