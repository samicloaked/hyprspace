package connectivity

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	drclient "github.com/ipfs/boxo/routing/http/client"
	"github.com/ipfs/boxo/routing/http/contentrouter"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/pnet"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/backoff"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	routedhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

const userAgent = "hyprspace-connectivity"

// httpRoutingWrapper adapts the delegated routing client so it satisfies routing.Routing.
type httpRoutingWrapper struct {
	routing.ContentRouting
	routing.PeerRouting
	routing.ValueStore
}

func (c *httpRoutingWrapper) Bootstrap(context.Context) error { return nil }

// CreateConnectivityNode assembles a libp2p host configured with NAT traversal,
// relay services, AutoRelay, and DHT discovery primitives.
func CreateConnectivityNode(ctx context.Context, priv crypto.PrivKey, bootstrap []peer.AddrInfo) (host.Host, *dht.IpfsDHT, error) {
	listenAddrs := loadListenAddrsFromEnv()

	peerChan := make(chan peer.AddrInfo)

	var opts []libp2p.Option
	if len(listenAddrs) > 0 {
		opts = append(opts, libp2p.ListenAddrs(listenAddrs...))
	}

	if opt, err := loadSwarmKeyOption(); err != nil {
		return nil, nil, err
	} else if opt != nil {
		opts = append(opts, opt)
	}

	opts = append(opts,
		libp2p.Identity(priv),
		libp2p.UserAgent(userAgent),
		libp2p.DefaultSecurity,
		libp2p.DefaultMuxers,
		libp2p.NATPortMap(),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.EnableHolePunching(),
		libp2p.EnableRelayService(relay.WithLimit(nil)),
		libp2p.EnableAutoRelayWithPeerSource(
			func(inner context.Context, numPeers int) <-chan peer.AddrInfo {
				r := make(chan peer.AddrInfo)
				go func() {
					defer close(r)
					for ; numPeers != 0; numPeers-- {
						select {
						case p, ok := <-peerChan:
							if !ok {
								return
							}
							select {
							case r <- p:
							case <-inner.Done():
								return
							}
						case <-inner.Done():
							return
						}
					}
				}()
				return r
			},
			autorelay.WithNumRelays(2),
			autorelay.WithBootDelay(10*time.Second),
		),
		libp2p.EnableAutoNATv2(),
		libp2p.EnableNATService(),
		libp2p.WithDialTimeout(5*time.Second),
		libp2p.FallbackDefaults,
	)

	basicHost, err := libp2p.New(opts...)
	if err != nil {
		return nil, nil, err
	}

	staticBootstrap, err := gatherBootstrapPeers(bootstrap)
	if err != nil {
		basicHost.Close()
		return nil, nil, err
	}

	if extra, err := fetchExtraPeersFromIPFS(); err == nil {
		fmt.Printf("[+] %d additional peers discovered via delegated API\n", len(extra))
		for _, ai := range extra {
			if ai.ID == basicHost.ID() {
				continue
			}
			basicHost.Peerstore().AddAddrs(ai.ID, ai.Addrs, 5*time.Minute)
		}
	} else if err != errNoIPFSAPI {
		fmt.Printf("[!] Failed to fetch additional peers: %s\n", err)
	}

	dhtOut, err := dht.New(
		ctx,
		basicHost,
		dht.Mode(dht.ModeClient),
		dht.BootstrapPeers(staticBootstrap...),
		dht.BootstrapPeersFunc(func() []peer.AddrInfo {
			dynamic, err := fetchExtraBootstrapFromIPFS()
			if err != nil {
				if err != errNoIPFSAPI {
					fmt.Printf("[!] Failed to refresh delegated bootstrap list: %s\n", err)
				}
				return staticBootstrap
			}
			fmt.Printf("[+] %d delegated bootstrap peers available\n", len(dynamic))
			return append(staticBootstrap, dynamic...)
		}),
	)
	if err != nil {
		basicHost.Close()
		return nil, nil, err
	}

	if err := dhtOut.Bootstrap(ctx); err != nil {
		basicHost.Close()
		return nil, nil, err
	}

	connectToPeers(ctx, basicHost, staticBootstrap)

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 500
	transport.MaxIdleConnsPerHost = 100
	delegateHTTPClient := &http.Client{
		Transport: &drclient.ResponseBodyLimitedTransport{
			RoundTripper: transport,
			LimitBytes:   1 << 20,
		},
	}

	var routed host.Host
	if dr, err := drclient.New(
		"https://p2p.privatevoid.net",
		drclient.WithHTTPClient(delegateHTTPClient),
		drclient.WithIdentity(priv),
		drclient.WithUserAgent(userAgent),
	); err == nil {
		cr := contentrouter.NewContentRoutingClient(dr)
		router := parallelRouting{routings: []routedhost.Routing{dhtOut, httpRoutingWrapper{
			ContentRouting: cr,
			PeerRouting:    cr,
			ValueStore:     cr,
		}}}
		routed = routedhost.Wrap(basicHost, router)
	} else {
		fmt.Printf("[!] Delegated routing disabled: %s\n", err)
		routed = routedhost.Wrap(basicHost, dhtOut)
	}

	startAutoRelayFeeder(ctx, routed, peerChan, staticBootstrap)

	return routed, dhtOut, nil
}

func startAutoRelayFeeder(ctx context.Context, h host.Host, peerChan chan peer.AddrInfo, seeds []peer.AddrInfo) {
	go func() {
		defer close(peerChan)

		seedSet := make(map[peer.ID]struct{})
		for _, seed := range seeds {
			if seed.ID == h.ID() {
				continue
			}
			seedSet[seed.ID] = struct{}{}
			select {
			case peerChan <- seed:
			case <-ctx.Done():
				return
			}
		}

		delayFactory := backoff.NewExponentialDecorrelatedJitter(time.Second, time.Minute, 5.0, rand.NewSource(time.Now().UnixNano()))
		strategy := delayFactory()
		ticker := time.NewTicker(strategy.Delay())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pushPeers := func(peers []peer.ID) bool {
					pushed := false
					for _, pid := range peers {
						if pid == h.ID() {
							continue
						}
						pi := h.Peerstore().PeerInfo(pid)
						if len(pi.Addrs) == 0 {
							continue
						}
						if _, seen := seedSet[pi.ID]; !seen {
							seedSet[pi.ID] = struct{}{}
						}
						select {
						case peerChan <- pi:
							pushed = true
						case <-ctx.Done():
							return false
						}
					}
					return pushed
				}

				currentPeers := h.Network().Peers()
				pushed := pushPeers(currentPeers)
				if !pushed {
					// also scan known peers in peerstore
					pushPeers(h.Peerstore().Peers())
				}

				next := strategy.Delay()
				ticker.Reset(next)
			}
		}
	}()
}

type parallelRouting struct {
	routings []routedhost.Routing
}

func (pr parallelRouting) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	var wg sync.WaitGroup
	var mutex sync.Mutex
	info := peer.AddrInfo{ID: id}
	subCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for _, routing := range pr.routings {
		wg.Add(1)
		r := routing
		go func() {
			defer wg.Done()
			ai, err := r.FindPeer(subCtx, id)
			if err != nil {
				return
			}
			if ai.ID != id {
				return
			}
			mutex.Lock()
			info.Addrs = append(info.Addrs, ai.Addrs...)
			mutex.Unlock()
		}()
	}

	wg.Wait()
	if len(info.Addrs) == 0 {
		return info, routing.ErrNotFound
	}
	return info, nil
}

func loadSwarmKeyOption() (libp2p.Option, error) {
	swarmKeyFile, ok := os.LookupEnv("HYPRSPACE_SWARM_KEY")
	if !ok || swarmKeyFile == "" {
		return nil, nil
	}

	fmt.Println("[+] Using swarm key", swarmKeyFile)
	swarmKey, err := os.Open(swarmKeyFile)
	if err != nil {
		return nil, err
	}
	defer swarmKey.Close()
	key, err := pnet.DecodeV1PSK(swarmKey)
	if err != nil {
		return nil, err
	}
	return libp2p.PrivateNetwork(key), nil
}

func loadListenAddrsFromEnv() []ma.Multiaddr {
	listenStr, ok := os.LookupEnv("HYPRSPACE_LISTEN_ADDRESSES")
	if !ok || listenStr == "" {
		return nil
	}
	addrs, err := ParseListenAddrs(listenStr)
	if err != nil {
		fmt.Printf("[!] Failed to parse HYPRSPACE_LISTEN_ADDRESSES: %s\n", err)
		return nil
	}
	return addrs
}

func connectToPeers(ctx context.Context, h host.Host, peers []peer.AddrInfo) {
	for _, pi := range peers {
		if pi.ID == h.ID() {
			continue
		}
		h.Peerstore().AddAddrs(pi.ID, pi.Addrs, 5*time.Minute)
		go func(info peer.AddrInfo) {
			if err := h.Connect(ctx, info); err != nil {
				fmt.Printf("[!] Failed to connect to bootstrap %s: %s\n", info.ID, err)
			}
		}(pi)
	}
}
