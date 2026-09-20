package main

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
)

type sipResponseEnvelope struct {
	resp sipResponse
	peer *net.UDPAddr
}

type sipTransport struct {
	conn           *net.UDPConn
	trustedIP      net.IP
	mu             sync.RWMutex
	waiters        map[string]chan sipResponseEnvelope
	requestHandler func(sipRequest, *net.UDPAddr)
}

func newSIPTransport(listen, server string) (*sipTransport, error) {
	laddr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(server, ":") {
		server += ":5060"
	}
	raddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &sipTransport{conn: conn, trustedIP: raddr.IP, waiters: make(map[string]chan sipResponseEnvelope)}, nil
}

func (t *sipTransport) LocalAddr() *net.UDPAddr                            { return t.conn.LocalAddr().(*net.UDPAddr) }
func (t *sipTransport) SetRequestHandler(h func(sipRequest, *net.UDPAddr)) { t.requestHandler = h }
func (t *sipTransport) Close() error                                       { return t.conn.Close() }
func (t *sipTransport) WriteToUDP(b []byte, peer *net.UDPAddr) (int, error) {
	return t.conn.WriteToUDP(b, peer)
}

func (t *sipTransport) registerWaiter(callID string) (<-chan sipResponseEnvelope, func()) {
	ch := make(chan sipResponseEnvelope, 8)
	t.mu.Lock()
	t.waiters[callID] = ch
	t.mu.Unlock()
	return ch, func() { t.mu.Lock(); delete(t.waiters, callID); t.mu.Unlock() }
}

func (t *sipTransport) run(ctx context.Context) {
	go func() { <-ctx.Done(); _ = t.conn.Close() }()
	buf := make([]byte, 65535)
	for {
		n, peer, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("SIP transport read error: %v", err)
			continue
		}
		if t.trustedIP != nil && !peer.IP.Equal(t.trustedIP) {
			log.Printf("SIP packet rejected from untrusted peer=%s", peer)
			continue
		}
		raw := string(buf[:n])
		if strings.HasPrefix(raw, "SIP/2.0") {
			resp, err := parseSIPResponse(raw)
			if err != nil {
				continue
			}
			callID := resp.header("call-id")
			t.mu.RLock()
			ch := t.waiters[callID]
			t.mu.RUnlock()
			if ch != nil {
				select {
				case ch <- sipResponseEnvelope{resp: resp, peer: peer}:
				default:
				}
			}
			continue
		}
		req, err := parseSIPRequest(raw)
		if err != nil {
			continue
		}
		if t.requestHandler != nil {
			t.requestHandler(req, peer)
		}
	}
}
