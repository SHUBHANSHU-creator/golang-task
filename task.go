package main

import (
    "fmt"
    "net/http"
    "strings"
    "strconv"
    "time"
    "sync"
)

var (
    data   = make(map[string]Item)
    mutex  sync.RWMutex
)

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

    command := r.PostFormValue("command")
    parts := strings.Fields(command)

    if len(parts) != 4 || strings.ToUpper(parts[0]) != "SET" {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, "Invalid command format")
        return
    }

    key := parts[1]
    value := parts[2]
    expirationStr := parts[3]

    // Parse the expiration time
    expiration, err := strconv.Atoi(expirationStr)
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprintf(w, "Invalid expiration time")
        return
    }

    // Calculate the expiration time
    expirationTime := time.Now().Add(time.Duration(expiration) * time.Second)

    // Store the key-value pair with expiration in the database
    mutex.Lock()
    data[key] = Item{Value: value, Expiration: expirationTime}
    mutex.Unlock()

    fmt.Fprintf(w, "Key '%s' set successfully with a %d-second expiration time", key, expiration)
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

    fmt.Fprintf(w, "Key: %s, Value: %s", key, item.Value)
}
