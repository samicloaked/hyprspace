//go:build connectivity_main

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hyprspace/hyprspace/connectivity"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Main is gated behind the `connectivity_main` build tag so that the library can
// coexist with this example inside the same module.
func main() {
	identityPath := flag.String("identity", "", "Path to a libp2p private key (created if missing).")
	bootstrapStr := flag.String("bootstrap", "", "Comma-separated bootstrap peer multiaddrs.")
	rendezvous := flag.String("rendezvous", connectivity.DefaultRendezvous, "Rendezvous string for DHT discovery.")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("[-] Received shutdown signal")
		cancel()
	}()

	priv, err := connectivity.LoadOrCreateIdentity(*identityPath)
	if err != nil {
		fmt.Printf("[!] Failed to load identity: %s\n", err)
		os.Exit(1)
	}

	var bootstrap []peer.AddrInfo
	if trimmed := strings.TrimSpace(*bootstrapStr); trimmed != "" {
		addrs := strings.Split(trimmed, ",")
		bootstrap, err = connectivity.ParsePeerAddrs(addrs)
		if err != nil {
			fmt.Printf("[!] Failed to parse bootstrap peers: %s\n", err)
			os.Exit(1)
		}
	}

	host, dht, err := connectivity.CreateConnectivityNode(ctx, priv, bootstrap)
	if err != nil {
		fmt.Printf("[!] Failed to create connectivity node: %s\n", err)
		os.Exit(1)
	}
	defer host.Close()
	if dht != nil {
		defer dht.Close()
	}

	fmt.Printf("[-] Local peer ID: %s\n", host.ID())
	fmt.Println("[-] Listening addresses:")
	for _, addr := range host.Addrs() {
		fmt.Printf("    %s/p2p/%s\n", addr, host.ID())
	}

	if err := connectivity.StartConnectivityEventLogger(ctx, host); err != nil {
		fmt.Printf("[!] Failed to start event logger: %s\n", err)
	}

	connectivity.StartDiscovery(ctx, host, dht, *rendezvous)

	<-ctx.Done()
	fmt.Println("[-] Shutting down connectivity node")
}
