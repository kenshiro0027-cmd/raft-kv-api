package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

// main.goと同じ型定義をここにも書く
type PutArgs struct{ Key, Val string }
type GetArgs struct{ Key string }
type GetReply struct {
	Value string
	Found bool
	All   map[string]string
}
type SimpleReply struct {
	OK  bool
	Msg string
}
type StateReply struct {
	ID, Role, Term, LastIndex, Applied, Addr string
}

func rpcClient(addr string) (*rpc.Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func main() {
	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val := r.URL.Query().Get("value")

		client, err := rpcClient("127.0.0.1:9101")
		if err != nil {
			http.Error(w, "RPC接続失敗: "+err.Error(), 500)
			return
		}
		defer client.Close()

		var reply SimpleReply
		err = client.Call("API.Put", &PutArgs{Key: key, Val: val}, &reply)
		if err != nil {
			http.Error(w, "Put失敗: "+err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(reply)
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		client, err := rpcClient("127.0.0.1:9101")
		if err != nil {
			http.Error(w, "RPC接続失敗: "+err.Error(), 500)
			return
		}
		defer client.Close()

		var reply GetReply
		err = client.Call("API.Get", &GetArgs{Key: key}, &reply)
		if err != nil {
			http.Error(w, "Get失敗: "+err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(reply)
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		client, err := rpcClient("127.0.0.1:9101")
		if err != nil {
			http.Error(w, "RPC接続失敗: "+err.Error(), 500)
			return
		}
		defer client.Close()

		var reply StateReply
		err = client.Call("API.State", struct{}{}, &reply)
		if err != nil {
			http.Error(w, "State失敗: "+err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(reply)
	})

	fmt.Println("Bridgeサーバー起動: http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
