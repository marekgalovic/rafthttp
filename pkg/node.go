package pkg

import (
    "fmt";
    "context";
    "math";
    "time";
    "bytes";
    "net/http";
    "encoding/binary";
    "math/rand";
    "io";

    "go.etcd.io/etcd/raft";
    "go.etcd.io/etcd/raft/raftpb";
    "github.com/satori/go.uuid";
    log "github.com/Sirupsen/logrus";
)

type node struct {
    s *server
    config *raft.Config
    raft raft.Node
    storage *raft.MemoryStorage
    kv map[string]string
    nodeAddresses map[uint64]string
    commitNotifiers map[uuid.UUID]chan struct{}
}

func NewNode(s *server, address string, join string) *node {
    storage := raft.NewMemoryStorage()
    c :=  &raft.Config {
        ID: uint64(time.Now().UTC().UnixNano()),
        ElectionTick: 10,
        HeartbeatTick: 1,
        Storage: storage,
        MaxSizePerMsg: math.MaxUint16,
        MaxInflightMsgs: 256,
    }

    var peers []raft.Peer
    if len(join) == 0 {
        peers = []raft.Peer{{ID: c.ID, Context: []byte(address)}}   
    }

    log.Infof("NewNode: %d", c.ID)
    n := &node {
        s: s,
        config: c, 
        raft: raft.StartNode(c, peers),
        storage: storage,
        kv: make(map[string]string),
        nodeAddresses: make(map[uint64]string),
        commitNotifiers: make(map[uuid.UUID]chan struct{}),
    }

    s.registerNode(n)

    if len(join) > 0 {
        buf := bytes.NewBuffer(nil)
        binary.Write(buf, binary.LittleEndian, c.ID)
        buf.Write([]byte(address))

        resp, err := http.Post("http://127.0.0.1" + join + "/join", "bytes", buf)
        if err != nil {
            log.Fatal(err)
        }

        var numAddresses int32
        if err := binary.Read(resp.Body, binary.LittleEndian, &numAddresses); err != nil {
            log.Fatal(err)
        }

        for i := 0; i < int(numAddresses); i++ {
            var id uint64
            var addressBytes int32
            if err := binary.Read(resp.Body, binary.LittleEndian, &id); err != nil {
                log.Fatal(err)
            }
            if err := binary.Read(resp.Body, binary.LittleEndian, &addressBytes); err != nil {
                log.Fatal(err)
            }

            address := make([]byte, addressBytes)
            nRead, err := resp.Body.Read(address)
            if err == io.EOF && nRead == int(addressBytes) {
                n.nodeAddresses[id] = string(address)
                break
            } else if err != nil {
                log.Fatal(err)
            }

            n.nodeAddresses[id] = string(address)
        }

        log.Info(n.nodeAddresses)
    }

    return n
}

func (n *node) Set(ctx context.Context, key []byte, value []byte) error {
    id, err := uuid.NewV4()
    if err != nil {
        return err
    }

    notifier := make(chan struct{})
    n.commitNotifiers[id] = notifier

    proposedAt := time.Now()
    if err := n.raft.Propose(ctx, bytes.Join([][]byte{id.Bytes(), key, value}, []byte(":"))); err != nil {
        return err
    }

    select {
    case <- notifier:
        log.Infof("Commit notification received. %s", time.Since(proposedAt))
    case <- ctx.Done():
        return fmt.Errorf("Commit not received")
    }

    return nil
}

func(n *node) Run(ctx context.Context) {
    ticker := time.NewTicker(time.Duration(rand.Intn(100) + 100) * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <- ticker.C:
            n.raft.Tick()
        case rd := <- n.raft.Ready():
            n.save(rd.HardState, rd.Entries, rd.Snapshot)
            
            for _, entry := range rd.CommittedEntries {
                switch entry.Type {
                case raftpb.EntryNormal:
                    n.process(entry)
                case raftpb.EntryConfChange:
                    var cc raftpb.ConfChange
                    cc.Unmarshal(entry.Data)
                    n.raft.ApplyConfChange(cc)

                    log.Infof("Node: %d, conf change: %v, applied: %d", n.config.ID, cc, n.raft.Status().Applied)
                    switch cc.Type {
                    case raftpb.ConfChangeAddNode:
                        n.nodeAddresses[cc.NodeID] = string(cc.Context)
                    case raftpb.ConfChangeRemoveNode:
                        delete(n.nodeAddresses, cc.NodeID)
                    }
                }
            }

            if err := n.send(ctx, rd.Messages); err != nil {
                panic(err)
            }

            n.raft.Advance()
        case <- ctx.Done():
            return
        }
    }
}

func (n *node) save(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
    n.storage.Append(entries)

    if !raft.IsEmptyHardState(hardState) {
        n.storage.SetHardState(hardState)
    }

    if !raft.IsEmptySnap(snapshot) {
        n.storage.ApplySnapshot(snapshot)
    }
}

func (n *node) process(entry raftpb.Entry) {
    if entry.Data == nil {
        return
    }

    idkv := bytes.SplitN(entry.Data, []byte(":"), 3)
    n.kv[string(idkv[1])] = string(idkv[2])

    notifier, exists := n.commitNotifiers[uuid.Must(uuid.FromBytes(idkv[0]))]
    if exists {
        select {
        case notifier <- struct{}{}:
        default:
        }
    }
    log.Info(n.kv)
}

func (n *node) send(ctx context.Context, messages []raftpb.Message) error {
    for _, m := range messages {
        address, exists := n.nodeAddresses[m.To]
        if !exists {
            log.Info(m)
            log.Errorf("Node id: %d not found", m.To)
            n.raft.ReportUnreachable(m.To)
            return nil
        }

        data, err := m.Marshal()
        if err != nil {
            return err
        }

        _, err = http.Post("http://127.0.0.1" + address + "/message", "bytes", bytes.NewBuffer(data))
        if err != nil {
            log.Errorf("Failed to send message to: %d. %v", m.To, err)
            n.raft.ReportUnreachable(m.To)

            cctx, _ := context.WithTimeout(ctx, 2 * time.Second)
            n.raft.ProposeConfChange(cctx, raftpb.ConfChange {
                NodeID: m.To,
                Type: raftpb.ConfChangeRemoveNode,
            })
        }
    }
    return nil
}

func (n *node) receive(ctx context.Context, message raftpb.Message) {
    n.raft.Step(ctx, message)
}