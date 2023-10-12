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
	//data map used to store the key value pairs
	data = make(map[string]Item)
	//Queue data structure made
	Queue = make(map[string][]int)
	mutex sync.RWMutex
)

type Command struct {
	Command string `json:"command"`
}

type Item struct {
	Value      string
	Expiration time.Time
}

type QPushCommand struct {
	Command string `json:"command"`
}

type QPopCommand struct {
	Command string `json:"command"`
}

type QPopResponse struct {
	Value int `json:"value"`
}

type BQPopCommand struct {
	Command string  `json:"command"`
	Timeout float64 `json:"timeout"`
}

type BQPopResponse struct {
	Value interface{} `json:"value"`
}

func main() {
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/qpush", handleQPUSH)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/qpop", handleQPOP)
	http.HandleFunc("/bqpop", handleBQPOP)
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
					fmt.Fprintf(w, "Key  set successfully")
				} else {
					fmt.Fprintf(w, "Key  was not present")
				}
			}
		}

	} else {
		// Store the key-value pair with expiration in the database
		mutex.Lock()
		data[key] = Item{Value: value, Expiration: expirationTime}
		mutex.Unlock()
		fmt.Fprintf(w, "Key  set successfully")
	}

}

func handleGet(w http.ResponseWriter, r *http.Request) {
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

	if len(parts) != 2 || strings.ToUpper(parts[0]) != "GET" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid command format")
		return
	}

	key := parts[1]

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

func handleQPOP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Method not allowed")
		return
	}

	decoder := json.NewDecoder(r.Body)
	var command QPopCommand

	err := decoder.Decode(&command)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid JSON format")
		return
	}
	parts := strings.Fields(command.Command)
	key := parts[1]

	mutex.Lock()
	defer mutex.Unlock()

	// Check if the array already exists in the map
	existingArray, exists := Queue[key]
	if exists {
		// Pop the last value from the existing array
		var poppedValue int
		if len(existingArray) > 0 {
			poppedValue, existingArray = existingArray[len(existingArray)-1], existingArray[:len(existingArray)-1]
			Queue[key] = existingArray
		} else {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Array is empty")
			return
		}

		response := QPopResponse{Value: poppedValue}
		jsonResponse, _ := json.Marshal(response)

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Array not found for key: %s", key)
	}
}

func handleBQPOP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Method not allowed")
		return
	}

	decoder := json.NewDecoder(r.Body)
	var command BQPopCommand

	err := decoder.Decode(&command)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid JSON format")
		return
	}

	parts := strings.Fields(command.Command)

	if len(parts) != 3 || strings.ToUpper(parts[0]) != "BQPOP" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid command format")
		return
	}

	key := parts[1]
	mutex.Lock()
	array, exists := Queue[key]
	if !exists {
		createArray(key)
		array = Queue[key]
	}
	mutex.Unlock()

	fmt.Println(key, array)
	timeoutDuration := time.Duration(int(command.Timeout*1000)) * time.Millisecond

	// Use a channel to synchronize access to the queue with timeout
	ch := make(chan interface{}, 1)

	go func() {
		mutex.Lock()
		defer mutex.Unlock()
		if len(array) > 0 {
			value := array[len(array)-1]
			Queue[key] = array[:len(array)-1]
			ch <- value
		} else {
			ch <- nil
		}
	}()

	select {
	case value := <-ch:
		response := BQPopResponse{Value: value}
		jsonResponse, _ := json.Marshal(response)

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
	case <-time.After(timeoutDuration):
		response := BQPopResponse{Value: nil}
		jsonResponse, _ := json.Marshal(response)

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonResponse)
	}
}

func createArray(key string) {
	Queue[key] = make([]int, 0)
}

func handleQPUSH(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, "Method not allowed")
		return
	}

	decoder := json.NewDecoder(r.Body)
	var command QPushCommand

	err := decoder.Decode(&command)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid JSON format")
		return
	}

	parts := strings.Fields(command.Command)

	if len(parts) < 3 || strings.ToUpper(parts[0]) != "QPUSH" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid command format")
		return
	}

	key := parts[1]
	valueStr := strings.Join(parts[2:], " ")

	values := parseValues(valueStr)

	mutex.Lock()
	array, exists := Queue[key]
	if !exists {
		createArray(key)
		array = Queue[key]
	}
	array = append(array, values...)
	Queue[key] = array

	mutex.Unlock()
	fmt.Fprintf(w, "Values appended to array %s: %v", key, Queue[key])
}

func parseValues(valueStr string) []int {
	parts := strings.Fields(valueStr)
	values := make([]int, 0, len(parts))

	for _, part := range parts {
		if intValue, err := strconv.Atoi(part); err == nil {
			values = append(values, intValue)
		}
	}

	return values
}
