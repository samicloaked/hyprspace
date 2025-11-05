package connectivity

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
)

// StartConnectivityEventLogger attaches loggers for peer connectivity and reachability events.
func StartConnectivityEventLogger(ctx context.Context, h host.Host) error {
	if err := logPeerConnectedness(ctx, h); err != nil {
		return err
	}
	if err := logReachability(ctx, h); err != nil {
		return err
	}
	if err := logNATType(ctx, h); err != nil {
		return err
	}
	return nil
}

func logPeerConnectedness(ctx context.Context, h host.Host) error {
	sub, err := h.EventBus().Subscribe(new(event.EvtPeerConnectednessChanged), eventbus.BufSize(256))
	if err != nil {
		return err
	}
	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-sub.Out():
				if !ok {
					return
				}
				evt := e.(event.EvtPeerConnectednessChanged)
				switch evt.Connectedness {
				case network.Connected:
					for _, c := range h.Network().ConnsToPeer(evt.Peer) {
						fmt.Printf("[+] Connected to %s/p2p/%s\n", c.RemoteMultiaddr(), evt.Peer)
					}
				case network.NotConnected:
					fmt.Printf("[!] Disconnected from %s\n", evt.Peer)
				case network.Limited:
					fmt.Printf("[~] Limited connection to %s\n", evt.Peer)
				}
			}
		}
	}()
	return nil
}

func logReachability(ctx context.Context, h host.Host) error {
	sub, err := h.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged), eventbus.BufSize(32))
	if err != nil {
		return err
	}
	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-sub.Out():
				if !ok {
					return
				}
				evt := e.(event.EvtLocalReachabilityChanged)
				switch evt.Reachability {
				case network.ReachabilityPublic:
					fmt.Println("[+] Reachability: public (direct dialing enabled)")
				case network.ReachabilityPrivate:
					fmt.Println("[!] Reachability: private (relying on relays/hole-punching)")
				case network.ReachabilityUnknown:
					fmt.Println("[-] Reachability: unknown")
				}
			}
		}
	}()
	return nil
}

func logNATType(ctx context.Context, h host.Host) error {
	sub, err := h.EventBus().Subscribe(new(event.EvtNATDeviceTypeChanged), eventbus.BufSize(32))
	if err != nil {
		return err
	}
	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-sub.Out():
				if !ok {
					return
				}
				evt := e.(event.EvtNATDeviceTypeChanged)
				fmt.Printf("[-] NAT device for %s detected as %s\n", evt.TransportProtocol, evt.NatDeviceType)
			}
		}
	}()
	return nil
}
