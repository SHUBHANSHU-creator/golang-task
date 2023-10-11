package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	data  = make(map[string]Item)
	mutex sync.RWMutex
)

type Command struct {
	Command string `json:"command"`
}

type Item struct {
	Value      string
	Expiration time.Time
}

func main() {
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.ListenAndServe(":8080", nil)
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Method not allowed")
		return
	}

	// Parse the JSON data from the request body
	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid JSON format")
		return
	}

	// Extract the command from the parsed JSON
	command := cmd.Command

	// Split the command into parts
	parts := strings.Fields(command)

	if len(parts) < 3 || strings.ToUpper(parts[0]) != "SET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid command format")
		return
	}

	key := parts[1]
	value := parts[2]
	// Check if expiration time is provided
	var expirationTime time.Time
	if len(parts) > 3 {
		//If only expiry condition is mentioned
		if len(parts) == 5 {
			expirationStr := parts[4]
			expiration, err := strconv.Atoi(expirationStr)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "Invalid expiration time")
				return
			}
			expirationTime = time.Now().Add(time.Duration(expiration) * time.Second)

			// Store the key-value pair with expiration in the database
			mutex.Lock()
			data[key] = Item{Value: value, Expiration: expirationTime}
			mutex.Unlock()

			fmt.Fprintf(w, "Key  set successfully")
		} else {
			//if optional condition also mentioned
			condition := parts[5]
			//If NX condition is provided
			if condition == "NX" {
				item, found := data[key]
				//checking if they key does not exisits or has expired
				if !found || time.Now().After(item.Expiration) {
					expirationStr := parts[4]
					expiration, err := strconv.Atoi(expirationStr)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						fmt.Fprintf(w, "Invalid expiration time")
						return
					}
					expirationTime = time.Now().Add(time.Duration(expiration) * time.Second)

					// Store the key-value pair with expiration in the database
					mutex.Lock()
					data[key] = Item{Value: value, Expiration: expirationTime}
					mutex.Unlock()

					fmt.Fprintf(w, "Key  set successfully")
				} else {
					fmt.Fprintf(w, "Key  was already present successfully")
				}
			} else {
				//If XX condition is provided
				item, found := data[key]
				if found || time.Now().Before(item.Expiration) || time.Now().Equal(item.Expiration) {
					expirationStr := parts[4]
					expiration, err := strconv.Atoi(expirationStr)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						fmt.Fprintf(w, "Invalid expiration time")
						return
					}
					expirationTime = time.Now().Add(time.Duration(expiration) * time.Second)

					// Store the key-value pair with expiration in the database
					mutex.Lock()
					data[key] = Item{Value: value, Expiration: expirationTime}
					mutex.Unlock()
				}
			}
		}

	} else {
		// Store the key-value pair with expiration in the database
		mutex.Lock()
		data[key] = Item{Value: value, Expiration: expirationTime}
		mutex.Unlock()
	}

}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Method not allowed")
		return
	}

	key := r.URL.Query().Get("key")

	mutex.RLock()
	item, found := data[key]
	mutex.RUnlock()

	if !found {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Key not found")
		return
	}

	if time.Now().After(item.Expiration) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Key has expired")
		return
	}

	fmt.Fprintf(w, "Key: %s, Value: %s,Time: %s", key, item.Value, item.Expiration)
}
