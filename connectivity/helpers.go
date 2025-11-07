package connectivity

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	coredisc "github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/backoff"
	rd "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	ma "github.com/multiformats/go-multiaddr"
)

var (
	errNoIPFSAPI = errors.New("ipfs api endpoint not configured")
)

// DefaultBootstrapPeers returns the built-in Hyprspace bootstrap peers.
func DefaultBootstrapPeers() []string {
	return []string{
		"/ip4/152.67.75.145/tcp/110/p2p/12D3KooWQWsHPUUeFhe4b6pyCaD1hBoj8j6Z7S7kTznRTh1p1eVt",
		"/ip4/152.67.75.145/udp/110/quic-v1/p2p/12D3KooWQWsHPUUeFhe4b6pyCaD1hBoj8j6Z7S7kTznRTh1p1eVt",
		"/ip4/152.67.75.145/tcp/995/p2p/QmbrAHuh4RYcyN9fWePCZMVmQjbaNXtyvrDCWz4VrchbXh",
		"/ip4/152.67.75.145/udp/995/quic-v1/p2p/QmbrAHuh4RYcyN9fWePCZMVmQjbaNXtyvrDCWz4VrchbXh",
		"/ip4/95.216.8.12/tcp/110/p2p/Qmd7QHZU8UjfYdwmjmq1SBh9pvER9AwHpfwQvnvNo3HBBo",
		"/ip4/95.216.8.12/udp/110/quic-v1/p2p/Qmd7QHZU8UjfYdwmjmq1SBh9pvER9AwHpfwQvnvNo3HBBo",
		"/ip4/95.216.8.12/tcp/995/p2p/QmYs4xNBby2fTs8RnzfXEk161KD4mftBfCiR8yXtgGPj4J",
		"/ip4/95.216.8.12/udp/995/quic-v1/p2p/QmYs4xNBby2fTs8RnzfXEk161KD4mftBfCiR8yXtgGPj4J",
		"/ip4/152.67.73.164/tcp/995/p2p/12D3KooWL84sAtq1QTYwb7gVbhSNX5ZUfVt4kgYKz8pdif1zpGUh",
		"/ip4/152.67.73.164/udp/995/quic-v1/p2p/12D3KooWL84sAtq1QTYwb7gVbhSNX5ZUfVt4kgYKz8pdif1zpGUh",
		"/ip4/37.27.11.202/udp/21/quic-v1/p2p/12D3KooWN31twBvdEcxz2jTv4tBfPe3mkNueBwDJFCN4xn7ZwFbi",
		"/ip4/37.27.11.202/udp/443/quic-v1/p2p/12D3KooWN31twBvdEcxz2jTv4tBfPe3mkNueBwDJFCN4xn7ZwFbi",
		"/ip4/37.27.11.202/udp/500/quic-v1/p2p/12D3KooWN31twBvdEcxz2jTv4tBfPe3mkNueBwDJFCN4xn7ZwFbi",
		"/ip4/37.27.11.202/udp/995/quic-v1/p2p/12D3KooWN31twBvdEcxz2jTv4tBfPe3mkNueBwDJFCN4xn7ZwFbi",
		"/dnsaddr/bootstrap.libp2p.io/p2p/12D3KooWEZXjE41uU4EL2gpkAQeDXYok6wghN7wwNVPF5bwkaNfS",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmZa1sAxajnQjVM8WjWXoMbmPd7NsWhfKsPkErzpm9wGkp",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	}
}

// ParsePeerAddrs turns multiaddr strings into peer.AddrInfo structures.
func ParsePeerAddrs(peers []string) ([]peer.AddrInfo, error) {
	var infos []peer.AddrInfo
	for _, addrStr := range peers {
		trimmed := strings.TrimSpace(addrStr)
		if trimmed == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(trimmed)
		if err != nil {
			return nil, err
		}
		pii, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, err
		}
		infos = append(infos, *pii)
	}
	return infos, nil
}

// ParseListenAddrs converts a comma separated set of multiaddrs into multiaddr structures.
func ParseListenAddrs(listen string) ([]ma.Multiaddr, error) {
	parts := strings.Split(listen, ",")
	var addrs []ma.Multiaddr
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(trimmed)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// LoadOrCreateIdentity loads an existing identity or creates a new one on disk.
func LoadOrCreateIdentity(path string) (crypto.PrivKey, error) {
	if path == "" {
		fmt.Println("[-] Generating ephemeral identity")
		priv, _, err := crypto.GenerateEd25519Key(crand.Reader)
		if err != nil {
			return nil, err
		}
		return priv, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			key, _, genErr := crypto.GenerateEd25519Key(crand.Reader)
			if genErr != nil {
				return nil, genErr
			}
			bytes, genErr := crypto.MarshalPrivateKey(key)
			if genErr != nil {
				return nil, genErr
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return nil, err
			}
			if writeErr := os.WriteFile(path, bytes, 0o600); writeErr != nil {
				return nil, writeErr
			}
			fmt.Println("[+] Generated new identity at", path)
			return key, nil
		}
		return nil, err
	}

	key, err := crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, err
	}
	fmt.Println("[-] Loaded identity from", path)
	return key, nil
}

// gatherBootstrapPeers combines default, supplied, and environment bootstrap peers.
func gatherBootstrapPeers(initial []peer.AddrInfo) ([]peer.AddrInfo, error) {
	peers := make([]peer.AddrInfo, 0, len(initial))
	if len(initial) > 0 {
		peers = append(peers, initial...)
	} else {
		defaults, err := ParsePeerAddrs(DefaultBootstrapPeers())
		if err != nil {
			return nil, err
		}
		peers = append(peers, defaults...)
	}

	if envPeers, err := parseEnvBootstrapPeers(); err != nil {
		return nil, err
	} else {
		peers = append(peers, envPeers...)
	}

	return dedupePeerInfos(peers), nil
}

func parseEnvBootstrapPeers() ([]peer.AddrInfo, error) {
	str, ok := os.LookupEnv("HYPRSPACE_BOOTSTRAP_PEERS")
	if !ok || strings.TrimSpace(str) == "" {
		return nil, nil
	}
	peers := strings.Split(str, ",")
	return ParsePeerAddrs(peers)
}

func dedupePeerInfos(peers []peer.AddrInfo) []peer.AddrInfo {
	type peerState struct {
		addrs map[string]struct{}
		idx   int
	}
	seen := make(map[peer.ID]*peerState)
	var result []peer.AddrInfo
	for _, info := range peers {
		if info.ID == "" {
			continue
		}
		state, ok := seen[info.ID]
		if !ok {
			state = &peerState{
				addrs: make(map[string]struct{}),
				idx:   len(result),
			}
			seen[info.ID] = state
			result = append(result, peer.AddrInfo{ID: info.ID})
		}
		for _, addr := range info.Addrs {
			key := string(addr.Bytes())
			if _, exists := state.addrs[key]; exists {
				continue
			}
			state.addrs[key] = struct{}{}
			result[state.idx].Addrs = append(result[state.idx].Addrs, addr)
		}
	}
	return result
}

func fetchExtraPeersFromIPFS() ([]peer.AddrInfo, error) {
	addr, err := ipfsAPIAddr()
	if err != nil {
		return nil, err
	}
	peers, err := getExtraPeers(addr)
	if err != nil {
		return nil, err
	}
	return ParsePeerAddrs(peers)
}

func fetchExtraBootstrapFromIPFS() ([]peer.AddrInfo, error) {
	addr, err := ipfsAPIAddr()
	if err != nil {
		return nil, err
	}
	peers, err := getExtraBootstrapNodes(addr)
	if err != nil {
		return nil, err
	}
	return ParsePeerAddrs(peers)
}

func ipfsAPIAddr() (ma.Multiaddr, error) {
	str, ok := os.LookupEnv("HYPRSPACE_IPFS_API")
	if !ok || strings.TrimSpace(str) == "" {
		return nil, errNoIPFSAPI
	}
	addr, err := ma.NewMultiaddr(str)
	if err != nil {
		return nil, err
	}
	return addr, nil
}

func getExtraPeers(addr ma.Multiaddr) ([]string, error) {
	ip4, err := addr.ValueForProtocol(ma.P_IP4)
	if err != nil {
		return nil, err
	}
	port, err := addr.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return nil, err
	}
	resp, err := http.PostForm("http://"+ip4+":"+port+"/api/v0/swarm/peers", url.Values{})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Peers []struct {
			Addr string      `json:"Addr"`
			Peer interface{} `json:"Peer"`
		} `json:"Peers"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	var result []string
	for _, entry := range payload.Peers {
		peerStr := fmt.Sprintf("%s/p2p/%v", entry.Addr, entry.Peer)
		result = append(result, peerStr)
	}
	return result, nil
}

func getExtraBootstrapNodes(addr ma.Multiaddr) ([]string, error) {
	ip4, err := addr.ValueForProtocol(ma.P_IP4)
	if err != nil {
		return nil, err
	}
	port, err := addr.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return nil, err
	}
	resp, err := http.PostForm("http://"+ip4+":"+port+"/api/v0/bootstrap", url.Values{})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Peers []string `json:"Peers"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	return payload.Peers, nil
}

const DefaultRendezvous = "hyprspace-connectivity"

// StartDiscovery advertises and searches for peers sharing the rendezvous string using the DHT.
func StartDiscovery(ctx context.Context, h host.Host, dht *dht.IpfsDHT, rendezvous string) {
	if dht == nil {
		fmt.Println("[!] DHT not available; skipping discovery")
		return
	}
	if rendezvous == "" {
		rendezvous = DefaultRendezvous
	}

	routingDiscovery := rd.NewRoutingDiscovery(dht)

	go advertiseLoop(ctx, routingDiscovery, rendezvous)
	go discoveryLoop(ctx, h, routingDiscovery, rendezvous)
}

func advertiseLoop(ctx context.Context, discovery *rd.RoutingDiscovery, rendezvous string) {
	backoffFactory := backoff.NewExponentialDecorrelatedJitter(time.Second*30, time.Hour, 2.0, mathrand.NewSource(time.Now().UnixNano()))
	strategy := backoffFactory()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ttl, err := discovery.Advertise(ctx, rendezvous)
		if err != nil {
			fmt.Printf("[!] Failed to advertise rendezvous %q: %s\n", rendezvous, err)
			delay := strategy.Delay()
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		if ttl <= 0 {
			ttl = time.Hour
		}
		fmt.Printf("[-] Advertised rendezvous %q (ttl %s)\n", rendezvous, ttl)
		half := ttl / 2
		if half < time.Minute {
			half = time.Minute
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(half):
		}
	}
}

func discoveryLoop(ctx context.Context, h host.Host, discovery *rd.RoutingDiscovery, rendezvous string) {
	backoffFactory := backoff.NewExponentialDecorrelatedJitter(time.Second*5, time.Minute*5, 2.0, mathrand.NewSource(time.Now().UnixNano()))
	strategy := backoffFactory()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ch, err := discovery.FindPeers(ctx, rendezvous, coredisc.Limit(64))
		if err != nil {
			fmt.Printf("[!] Rendezvous discovery error: %s\n", err)
		} else {
			for pi := range ch {
				if pi.ID == "" || pi.ID == h.ID() {
					continue
				}
				if len(pi.Addrs) == 0 {
					continue
				}
				h.Peerstore().AddAddrs(pi.ID, pi.Addrs, time.Minute*10)
				go func(info peer.AddrInfo) {
					if err := h.Connect(ctx, info); err != nil {
						fmt.Printf("[!] Failed to connect to discovered peer %s: %s\n", info.ID, err)
					}
				}(pi)
			}
		}
		delay := strategy.Delay()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
