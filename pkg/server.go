package pkg

import (
    "context";
    "fmt";
    "net/http";
    "io/ioutil"
    "encoding/binary";

    "go.etcd.io/etcd/raft/raftpb";
    log "github.com/Sirupsen/logrus";
)

type server struct {
    address string
    node *node
}

func writeErr(w http.ResponseWriter, err error) {
    w.WriteHeader(http.StatusInternalServerError)
    w.Write([]byte(fmt.Sprintf("%v", err)))
}

func NewServer(address string) *server {
    if len(address) == 0 {
        log.Fatal("Empty server address")
    }
    s := &server{address: address}

    mux := http.NewServeMux()
    mux.HandleFunc("/message", s.handleMessage)
    mux.HandleFunc("/join", s.handleJoin)

    go http.ListenAndServe(address, mux)
    return s
}

func (s *server) registerNode(n *node) {
    s.node = n
}

func (s *server) handleMessage(w http.ResponseWriter, req *http.Request) {
    data, err := ioutil.ReadAll(req.Body)
    if err != nil {
        writeErr(w, err)
        return  
    }

    var message raftpb.Message
    if err := message.Unmarshal(data); err != nil {
        writeErr(w, err)
        return
    }

    s.node.receive(context.Background(), message)
}

func (s *server) handleJoin(w http.ResponseWriter, req *http.Request) {
    data, err := ioutil.ReadAll(req.Body)
    if err != nil {
        writeErr(w, err)
        return
    }

    id := binary.LittleEndian.Uint64(data[:8])
    err = s.node.raft.ProposeConfChange(context.Background(), raftpb.ConfChange {
        NodeID: id,
        Context: data[8:],
    })

    if err != nil {
        writeErr(w, err)
        return
    }

    binary.Write(w, binary.LittleEndian, int32(len(s.node.nodeAddresses)))
    for id, address := range s.node.nodeAddresses {
        binary.Write(w, binary.LittleEndian, id)
        binary.Write(w, binary.LittleEndian, int32(len([]byte(address))))
        w.Write([]byte(address))
    }
}