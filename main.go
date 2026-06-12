// main.go
//
// 外部API: net/rpc（アプリ層） / 内部: HashiCorp Raft（合意層）
// 参考にしたGitHubコード（書き方・API出典, 該当箇所に個別コメントあり）:
//   - github.com/hashicorp/raft/config.go
//       * DefaultConfig / LocalID / 各タイムアウト・スナップショット関連の設定
//   - github.com/hashicorp/raft/raft.go
//       * NewRaft / BootstrapCluster / Apply / AddVoter / GetConfiguration / Stats / LeaderWithID
//   - github.com/hashicorp/raft/tcp_transport.go
//       * NewTCPTransport / LocalAddr
//   - github.com/hashicorp/raft/snapshot.go
//       * FileSnapshotStore / SnapshotSink（Persistの書き方の参考）
//   - github.com/hashicorp/raft/fsm.go
//       * FSM / FSMSnapshot / Apply / Snapshot / Restore のシグネチャ
//   - github.com/hashicorp/raft-boltdb/bolt_store.go
//       * BoltStore(LogStore/StableStore) の初期化
//
// 起動例：
//   # 最初の1台（ブートストラップ）
//   go run main.go --id=node1 --raft-addr=127.0.0.1:7001 --rpc=127.0.0.1:9101 --data-dir=./cluster --bootstrap
//   # 2台目以降（ブートストラップなし）
//   go run main.go --id=node2 --raft-addr=127.0.0.1:7002 --rpc=127.0.0.1:9102 --data-dir=./cluster
//   go run main.go --id=node3 --raft-addr=127.0.0.1:7003 --rpc=127.0.0.1:9103 --data-dir=./cluster

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// ------------------------------------
// Log Command Format
// （Raftログに積むコマンド。fsm.goのApplyシグネチャに合わせてデコード）
// 参考: fsm.go / raft.go Apply
// ------------------------------------
type command struct {
	Op  string `json:"op"` // "set" | "del"
	Key string `json:"key"`
	Val string `json:"val,omitempty"`
}

// ------------------------------------
// FSM
// 参考: fsm.go（Apply/Snapshot/Restore）
// ------------------------------------
type kvFSM struct {
	mu sync.RWMutex
	m  map[string]string
}

func newKVFSM() *kvFSM { return &kvFSM{m: make(map[string]string)} }

func (f *kvFSM) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return fmt.Errorf("fsm apply unmarshal: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch c.Op {
	case "set":
		f.m[c.Key] = c.Val
	case "del":
		delete(f.m, c.Key)
	default:
		return fmt.Errorf("unknown op: %s", c.Op)
	}
	return nil
}

func (f *kvFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	cp := make(map[string]string, len(f.m))
	for k, v := range f.m {
		cp[k] = v
	}
	return &kvSnapshot{data: cp}, nil
}

func (f *kvFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	dec := json.NewDecoder(rc)

	m := map[string]string{}
	if err := dec.Decode(&m); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.m = m
	return nil
}

// ------------------------------------
// FSMSnapshot
// 参考: snapshot.go（SnapshotSink への Persist）
// ------------------------------------
type kvSnapshot struct {
	data map[string]string
}

func (s *kvSnapshot) Persist(sink raft.SnapshotSink) error {
	enc, err := json.Marshal(s.data)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(enc); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *kvSnapshot) Release() {}

// ------------------------------------
// Node (Raft)
// ------------------------------------
type node struct {
	id        string
	raftAddr  string
	rpcAddr   string
	dataDir   string
	bootstrap bool

	raft *raft.Raft
	fsm  *kvFSM
}

func newNode(id, raftAddr, rpcAddr, dataDir string, bootstrap bool) (*node, error) {
	n := &node{
		id:        id,
		raftAddr:  raftAddr,
		rpcAddr:   rpcAddr,
		dataDir:   dataDir,
		bootstrap: bootstrap,
		fsm:       newKVFSM(),
	}

	// Config
	// 参考: config.go（DefaultConfig / LocalID / SnapshotInterval / SnapshotThreshold）
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.SnapshotInterval = 30 * time.Second
	cfg.SnapshotThreshold = 8
	// 実験用にタイムアウトを短縮したい場合は適宜変更（例）:
	// cfg.HeartbeatTimeout = 150 * time.Millisecond
	// cfg.ElectionTimeout  = 300 * time.Millisecond

	// Stores (Bolt + FileSnapshot)
	// 参考: raft-boltdb/bolt_store.go / snapshot.go
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir dataDir: %w", err)
	}
	boltPath := filepath.Join(dataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}
	logStore := boltStore
	stableStore := boltStore

	snapDir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir snapshots: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(snapDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	// Transport
	// 参考: tcp_transport.go（NewTCPTransport / LocalAddr）
	transport, err := raft.NewTCPTransport(n.raftAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// Raft
	// 参考: raft.go（NewRaft）
	r, err := raft.NewRaft(cfg, n.fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}
	n.raft = r

	// Bootstrap (first node only)
	// 参考: raft.go（BootstrapCluster）
	if n.bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      raft.ServerID(n.id),
				Address: transport.LocalAddr(),
			}},
		}
		f := n.raft.BootstrapCluster(config)
		if f.Error() != nil && f.Error() != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", f.Error())
		}
	}
	return n, nil
}

// ------------------------------------
// RPC API (App Layer)
// State / Peers / Join / Put / Del / Get / Snapshot
// ------------------------------------

type APIServer struct {
	N *node
}

// ---- Request / Reply types（client_example.go と一致） ----
type PutArgs struct{ Key, Val string }
type DelArgs struct{ Key string }
type GetArgs struct{ Key string }
type GetReply struct {
	Value string
	Found bool
	All   map[string]string
}
type JoinArgs struct{ ID, Addr string }
type SimpleReply struct {
	OK  bool
	Msg string
}
type StateReply struct {
	ID, Role, Term, LastIndex, Applied, Addr string // Addr = Raftアドレス
}
type PeersReply struct {
	Peers []struct {
		ID   string
		Addr string
	}
}

// Snapshot（手動）
// 参考: raft.go（Barrier / Snapshot）
func (s *APIServer) Snapshot(_ struct{}, reply *SimpleReply) error {
	if err := s.N.raft.Barrier(5 * time.Second).Error(); err != nil {
		return err
	}
	if err := s.N.raft.Snapshot().Error(); err != nil {
		return err
	}
	*reply = SimpleReply{OK: true, Msg: "OK"}
	return nil
}

// Put（Leaderのみ受理）
// 参考: raft.go（Apply / State / LeaderWithID）
func (s *APIServer) Put(args *PutArgs, reply *SimpleReply) error {
	if args.Key == "" {
		return errors.New("key required")
	}
	if s.N.raft.State() != raft.Leader {
		leaderAddr, _ := s.N.raft.LeaderWithID()
		reply.OK = false
		reply.Msg = fmt.Sprintf("not leader; leader=%s", string(leaderAddr))
		return errors.New("not leader")
	}

	cmd := command{Op: "set", Key: args.Key, Val: args.Val}
	b, _ := json.Marshal(cmd)
	f := s.N.raft.Apply(b, 5*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	*reply = SimpleReply{OK: true, Msg: "OK"}
	return nil
}

// Del（Leaderのみ受理）
func (s *APIServer) Del(args *DelArgs, reply *SimpleReply) error {
	if args.Key == "" {
		return errors.New("key required")
	}
	if s.N.raft.State() != raft.Leader {
		leaderAddr, _ := s.N.raft.LeaderWithID()
		reply.OK = false
		reply.Msg = fmt.Sprintf("not leader; leader=%s", string(leaderAddr))
		return errors.New("not leader")
	}

	cmd := command{Op: "del", Key: args.Key}
	b, _ := json.Marshal(cmd)
	f := s.N.raft.Apply(b, 5*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	*reply = SimpleReply{OK: true, Msg: "OK"}
	return nil
}

// Get（ローカルFSM参照。強い読みが必要ならBarrierを挟む）
// 参考: raft.go（Barrier）
func (s *APIServer) Get(args *GetArgs, reply *GetReply) error {
	// _ = s.N.raft.Barrier(2 * time.Second).Error() // 線形化読みが必要なら有効化

	s.N.fsm.mu.RLock()
	defer s.N.fsm.mu.RUnlock()

	if args.Key == "" {
		all := make(map[string]string, len(s.N.fsm.m))
		for k, v := range s.N.fsm.m {
			all[k] = v
		}
		reply.All, reply.Found = all, true
		return nil
	}

	val, ok := s.N.fsm.m[args.Key]
	reply.Value, reply.Found = val, ok
	return nil
}

// Join（Leaderのみ）
// 参考: raft.go（AddVoter）
func (s *APIServer) Join(args *JoinArgs, reply *SimpleReply) error {
	if s.N.raft.State() != raft.Leader {
		leaderAddr, _ := s.N.raft.LeaderWithID()
		reply.OK = false
		reply.Msg = fmt.Sprintf("not leader; leader=%s", string(leaderAddr))
		return errors.New("not leader")
	}
	if args.ID == "" || args.Addr == "" {
		return errors.New("id and addr required")
	}
	f := s.N.raft.AddVoter(raft.ServerID(args.ID), raft.ServerAddress(args.Addr), 0, 0)
	if err := f.Error(); err != nil {
		return err
	}
	*reply = SimpleReply{OK: true, Msg: "OK"}
	return nil
}

// State
// 参考: raft.go（Stats）
func (s *APIServer) State(_ struct{}, reply *StateReply) error {
	stats := s.N.raft.Stats()
	*reply = StateReply{
		ID:        s.N.id,
		Role:      stats["state"],
		Term:      stats["term"],
		LastIndex: stats["last_log_index"],
		Applied:   stats["applied_index"],
		Addr:      s.N.raftAddr,
	}
	return nil
}

// Peers
// 参考: raft.go（GetConfiguration）
func (s *APIServer) Peers(_ struct{}, reply *PeersReply) error {
	cfgF := s.N.raft.GetConfiguration()
	if err := cfgF.Error(); err != nil {
		return err
	}
	var out []struct {
		ID   string
		Addr string
	}
	for _, srv := range cfgF.Configuration().Servers {
		out = append(out, struct {
			ID   string
			Addr string
		}{ID: string(srv.ID), Addr: string(srv.Address)})
	}
	reply.Peers = out
	return nil
}

// ------------------------------------
// main
// ------------------------------------
func main() {
	var (
		id        = flag.String("id", "", "node id (unique)")
		raftAddr  = flag.String("raft-addr", "", "raft bind addr (e.g., 127.0.0.1:7001)")
		rpcAddr   = flag.String("rpc", "", "rpc addr (e.g., 127.0.0.1:9101)")
		dataDir   = flag.String("data-dir", "data", "data directory for this node")
		bootstrap = flag.Bool("bootstrap", false, "bootstrap the cluster (only first node)")
	)
	flag.Parse()

	if *id == "" || *raftAddr == "" || *rpcAddr == "" {
		log.Fatalf("required: --id --raft-addr --rpc")
	}

	// 早期Listen確認
	mustListen(*raftAddr)
	mustListen(*rpcAddr)

	nodeDir := filepath.Join(*dataDir, *id)
	n, err := newNode(*id, *raftAddr, *rpcAddr, nodeDir, *bootstrap)
	if err != nil {
		log.Fatalf("new node: %v", err)
	}

	// net/rpc サーバ
	api := &APIServer{N: n}
	if err := rpc.RegisterName("API", api); err != nil {
		log.Fatalf("rpc register: %v", err)
	}
	ln, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("rpc listen: %v", err)
	}
	log.Printf("[node %s] raft=%s rpc=%s data=%s bootstrap=%v",
		n.id, n.raftAddr, n.rpcAddr, nodeDir, n.bootstrap)

	// Accept loop
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("rpc accept: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	select {}
}

func mustListen(addr string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen check(%s): %v", addr, err)
	}
	_ = ln.Close()
}
