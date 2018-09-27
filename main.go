package main

import (
    "os";
    "os/signal";
    "syscall";
    "context";
    "flag";
    "time";

    rp "github.com/marekgalovic/rafthttp/pkg";

    log "github.com/Sirupsen/logrus";
)

func main() {
    var address string
    var join string

    flag.StringVar(&address, "address", ":7000", "Node address")
    flag.StringVar(&join, "join", "", "Cluster node to join")
    flag.Parse()

    ctx, cancelCtx := context.WithCancel(context.Background())
    defer cancelCtx()

    server := rp.NewServer(address)
    node := rp.NewNode(server, address, join)
    go node.Run(ctx)

    if len(join) == 0 {
        time.Sleep(10 * time.Second)
        node.Set(ctx, []byte("foo"), []byte("bar"))
    }

    log.Info("Wait")
    wait := make(chan os.Signal, 1)
    signal.Notify(wait, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
    <- wait
}