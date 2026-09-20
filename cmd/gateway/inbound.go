// SPDX-FileCopyrightText: 2026 2M Production Electrique
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type sipRequest struct {
	method  string
	uri     string
	version string
	headers map[string][]string
	body    string
}

func (r sipRequest) header(name string) string {
	v := r.headers[strings.ToLower(name)]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func parseSIPRequest(raw string) (sipRequest, error) {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	partsBody := strings.SplitN(normalized, "\n\n", 2)
	lines := strings.Split(partsBody[0], "\n")
	if len(lines) == 0 {
		return sipRequest{}, errors.New("empty SIP request")
	}
	first := strings.Fields(lines[0])
	if len(first) != 3 || !strings.HasPrefix(first[2], "SIP/") {
		return sipRequest{}, errors.New("not a SIP request")
	}
	r := sipRequest{method: first[0], uri: first[1], version: first[2], headers: map[string][]string{}}
	if len(partsBody) == 2 {
		r.body = partsBody[1]
	}
	for _, line := range lines[1:] {
		i := strings.IndexByte(line, ':')
		if i <= 0 {
			continue
		}
		n := strings.ToLower(strings.TrimSpace(line[:i]))
		v := strings.TrimSpace(line[i+1:])
		r.headers[n] = append(r.headers[n], v)
	}
	return r, nil
}

type inboundCallControl struct{ cancel context.CancelFunc }

type inboundRegistry struct {
	mu    sync.Mutex
	calls map[string]*inboundCallControl
}

func (g *gateway) handleSharedSIPRequest(req sipRequest, peer *net.UDPAddr) {
	if g.sipTransport == nil || g.inboundReg == nil {
		return
	}
	conn := g.sipTransport.conn
	callID := req.header("call-id")
	switch strings.ToUpper(req.method) {
	case "OPTIONS", "NOTIFY":
		_ = writeSIPResponse(conn, peer, req, 200, "OK", "", "")
	case "INVITE":
		if callID == "" {
			_ = writeSIPResponse(conn, peer, req, 400, "Bad Request", "", "")
			return
		}
		_ = writeSIPResponse(conn, peer, req, 100, "Trying", "", "")
		cctx, cancel := context.WithCancel(context.Background())
		g.inboundReg.mu.Lock()
		g.inboundReg.calls[callID] = &inboundCallControl{cancel: cancel}
		g.inboundReg.mu.Unlock()
		go func(req sipRequest, peer *net.UDPAddr, callID string) {
			defer func() {
				g.inboundReg.mu.Lock()
				delete(g.inboundReg.calls, callID)
				g.inboundReg.mu.Unlock()
				cancel()
			}()
			if err := g.handleInboundINVITE(cctx, conn, peer, req); err != nil {
				log.Printf("SIP inbound call failed: callid=%s error=%v", callID, err)
			}
		}(req, peer, callID)
	case "CANCEL":
		_ = writeSIPResponse(conn, peer, req, 200, "OK", "", "")
		g.inboundReg.mu.Lock()
		ctl := g.inboundReg.calls[callID]
		g.inboundReg.mu.Unlock()
		if ctl != nil {
			ctl.cancel()
		}
	case "BYE":
		// Established inbound dialogs stay on the shared SIP transport.
		// Acknowledge the BYE immediately, then cancel the per-call context so
		// media, WebRTC and the HPB virtual session are cleaned up by the call goroutine.
		_ = writeSIPResponse(conn, peer, req, 200, "OK", "", "")
		g.inboundReg.mu.Lock()
		ctl := g.inboundReg.calls[callID]
		g.inboundReg.mu.Unlock()
		if ctl != nil {
			log.Printf("SIP inbound BYE received: callid=%s", callID)
			ctl.cancel()
		}
	case "ACK":
		// ACK has no response.
	default:
		_ = writeSIPResponse(conn, peer, req, 405, "Method Not Allowed", "", "")
	}
}

func (g *gateway) isTrustedSIPPeer(peer *net.UDPAddr) bool {
	server := g.cfg.SIPServer
	if !strings.Contains(server, ":") {
		server += ":5060"
	}
	raddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil || raddr.IP == nil {
		return false
	}
	return peer.IP.Equal(raddr.IP)
}

func (g *gateway) handleInboundINVITE(ctx context.Context, listener *net.UDPConn, peer *net.UDPAddr, req sipRequest) error {
	caller := sipUser(req.header("p-asserted-identity"))
	if caller == "" {
		caller = sipUser(req.header("from"))
	}
	called := sipUser(req.uri)
	if called == "" {
		called = sipUser(req.header("to"))
	}
	if caller == "" {
		caller = "anonymous"
	}
	if called == "" {
		_ = writeSIPResponse(listener, peer, req, 400, "Bad Request", "", "")
		return errors.New("could not extract called number")
	}
	account, ok := g.accounts.AccountByExtension(called)
	if !ok {
		_ = writeSIPResponse(listener, peer, req, 404, "Not Found", "", "")
		return fmt.Errorf("no configured SIP account for called extension %s", called)
	}
	log.Printf("SIP INBOUND INVITE: from=%s to=%s nextcloud_user=%s peer=%s", caller, called, account.NextcloudUser, peer)

	room, err := g.directDialIn(ctx, called, caller)
	if err != nil {
		code := 500
		phrase := "Server Internal Error"
		if errors.Is(err, errDialInNotFound) {
			code = 404
			phrase = "Not Found"
		}
		if errors.Is(err, errDialInUnauthorized) {
			code = 403
			phrase = "Forbidden"
		}
		_ = writeSIPResponse(listener, peer, req, code, phrase, "", "")
		return err
	}
	callID := newID("sip-in")
	callSession, err := g.openTalkCallSession(room.Token, callID, caller, room.ActorID, room.ActorType)
	if err != nil {
		_ = writeSIPResponse(listener, peer, req, 500, "Server Internal Error", "", "")
		return err
	}
	defer callSession.Close()
	_ = writeSIPResponse(listener, peer, req, 180, "Ringing", "", "")
	log.Printf("SIP inbound ringing Talk user: room=%d token=%s caller=%s", room.ID, room.Token, caller)

	waitCtx, cancel := context.WithTimeout(ctx, g.cfg.SIPInboundTimeout)
	defer cancel()
	publisher, err := callSession.WaitForTalkParticipant(waitCtx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_ = writeSIPResponse(listener, peer, req, 480, "Temporarily Unavailable", "", "")
		}
		return fmt.Errorf("wait for Talk answer: %w", err)
	}
	log.Printf("Talk answered incoming phone call: room=%d session=%s", room.ID, publisher)

	remoteRTP, selectedPT, codec, err := parseSDPAnswer(req.body)
	if err != nil {
		_ = writeSIPResponse(listener, peer, req, 488, "Not Acceptable Here", "", "")
		return fmt.Errorf("parse incoming SDP: %w", err)
	}
	rtpLAddr, _ := net.ResolveUDPAddr("udp", g.cfg.RTPLocalAddr)
	rtpConn, err := net.ListenUDP("udp", rtpLAddr)
	if err != nil {
		return err
	}
	defer rtpConn.Close()
	contactIP := g.cfg.RTPAdvertiseIP
	if contactIP == "" {
		contactIP = localIPFor(peer)
	}
	rtpPort := rtpConn.LocalAddr().(*net.UDPAddr).Port

	// Keep the established SIP dialog on the same persistent transport used
	// for REGISTER and incoming INVITEs. This is required with Asterisk
	// rewrite_contact=yes and prevents ACK/BYE from being split across ports.
	toTag := newID("it")
	sipPort := listener.LocalAddr().(*net.UDPAddr).Port
	contactURI := fmt.Sprintf("sip:talk@%s:%d", contactIP, sipPort)
	sdp := buildSDPAnswer(contactIP, rtpPort, selectedPT)
	extra := fmt.Sprintf("Contact: <%s>\r\nContent-Type: application/sdp\r\n", contactURI)
	if err := writeSIPResponseWithTag(listener, peer, req, 200, "OK", toTag, extra, sdp); err != nil {
		return err
	}
	log.Printf("SIP inbound answered: room=%d RTP=%s:%d codec=%s/PT%d", room.ID, contactIP, rtpPort, codec, selectedPT)

	// Start the exact media bridge already validated for outbound calls. For inbound
	// calls the SDP offer is authoritative initially, while PublishSIPRTP still
	// accepts PT0/PT8 dynamically packet by packet.
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	defer bridgeCancel()
	bridge, err := callSession.StartMediaBridge(bridgeCtx, selectedPT, rtpConn, remoteRTP, selectedPT)
	if err != nil {
		return err
	}
	defer bridge.Close()

	mediaDone := make(chan struct{})
	go func() {
		defer close(mediaDone)
		b := make([]byte, 4096)
		for {
			_ = rtpConn.SetReadDeadline(time.Now().Add(time.Second))
			n, _, e := rtpConn.ReadFromUDP(b)
			if ne, ok := e.(net.Error); ok && ne.Timeout() {
				select {
				case <-bridgeCtx.Done():
					return
				default:
					continue
				}
			}
			if e != nil {
				return
			}
			if e = bridge.PublishSIPRTP(b[:n]); e != nil {
				log.Printf("SIP inbound RTP publish error: %v", e)
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// The shared SIP request handler cancels this context after a remote BYE/CANCEL.
			return nil
		case <-bridge.TalkEnded():
			// Talk side hung up. Initiate BYE on the established SIP dialog using
			// the same shared transport/contact advertised in the 200 OK.
			return sendInboundBYE(listener, peer, req, toTag, contactURI)
		case <-mediaDone:
			return errors.New("incoming RTP bridge stopped")
		}
	}
}

type directDialInRoom struct {
	ID        int64  `json:"id"`
	Token     string `json:"token"`
	ActorID   string `json:"actorId"`
	ActorType string `json:"actorType"`
	SessionID string `json:"sessionId"`
}

var errDialInNotFound = errors.New("direct dial-in number not assigned")
var errDialInUnauthorized = errors.New("direct dial-in authentication rejected")

func (g *gateway) directDialIn(ctx context.Context, phoneNumber, caller string) (directDialInRoom, error) {
	body, _ := json.Marshal(map[string]string{"phoneNumber": phoneNumber, "caller": caller})
	rndBytes := make([]byte, 32)
	if _, e := rand.Read(rndBytes); e != nil {
		return directDialInRoom{}, e
	}
	rnd := hex.EncodeToString(rndBytes)
	mac := hmac.New(sha256.New, []byte(g.cfg.SIPBridgeSecret))
	_, _ = mac.Write([]byte(rnd + phoneNumber))
	checksum := hex.EncodeToString(mac.Sum(nil))
	endpoint := strings.TrimRight(g.cfg.NextcloudURL, "/") + "/ocs/v2.php/apps/spreed/api/v4/room/direct-dial-in?format=json"
	hreq, e := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if e != nil {
		return directDialInRoom{}, e
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "application/json")
	hreq.Header.Set("OCS-APIRequest", "true")
	hreq.Header.Set("Talk-SIPBridge-Random", rnd)
	hreq.Header.Set("Talk-SIPBridge-Checksum", checksum)
	hreq.Header.Set("User-Agent", userAgent)
	resp, e := http.DefaultClient.Do(hreq)
	if e != nil {
		return directDialInRoom{}, e
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == 404 {
		return directDialInRoom{}, errDialInNotFound
	}
	if resp.StatusCode == 401 {
		return directDialInRoom{}, errDialInUnauthorized
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return directDialInRoom{}, fmt.Errorf("direct-dial-in HTTP %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var env struct {
		OCS struct {
			Data directDialInRoom `json:"data"`
			Meta struct {
				Status     string `json:"status"`
				StatusCode int    `json:"statuscode"`
				Message    string `json:"message"`
			} `json:"meta"`
		} `json:"ocs"`
	}
	if e = json.Unmarshal(raw, &env); e != nil {
		return directDialInRoom{}, fmt.Errorf("decode direct-dial-in response: %w", e)
	}
	if env.OCS.Meta.StatusCode >= 400 {
		return directDialInRoom{}, fmt.Errorf("direct-dial-in OCS %d: %s", env.OCS.Meta.StatusCode, env.OCS.Meta.Message)
	}
	if env.OCS.Data.ID == 0 {
		return directDialInRoom{}, fmt.Errorf("direct-dial-in returned no room id: %s", strings.TrimSpace(string(raw)))
	}
	log.Printf("Nextcloud direct-dial-in created room: id=%d token=%s called=%s caller=%s actor=%s/%s", env.OCS.Data.ID, env.OCS.Data.Token, phoneNumber, caller, env.OCS.Data.ActorType, env.OCS.Data.ActorID)
	return env.OCS.Data, nil
}

func (s *talkCallSession) WaitForTalkParticipant(ctx context.Context) (string, error) {
	for _, p := range s.initialSessions {
		if isRealTalkSession(p, s.sessionID, s.virtualID) {
			return p.SessionID, nil
		}
	}
	for {
		if deadline, ok := ctx.Deadline(); ok {
			_ = s.conn.SetReadDeadline(deadline)
		}
		var msg serverMessage
		if e := s.conn.ReadJSON(&msg); e != nil {
			return "", e
		}
		if msg.Type == "event" && msg.Event != nil && msg.Event.Target == "room" && msg.Event.Type == "join" {
			for _, p := range msg.Event.Join {
				s.initialSessions = append(s.initialSessions, p)
				if isRealTalkSession(p, s.sessionID, s.virtualID) {
					return p.SessionID, nil
				}
			}
		}
	}
}

func isRealTalkSession(p rtcRoomSession, selfID, virtualID string) bool {
	if p.SessionID == "" || p.SessionID == selfID || p.SessionID == virtualID {
		return false
	}
	if t, _ := p.User["type"].(string); t == "phone" {
		return false
	}
	return true
}

func buildSDPAnswer(ip string, port int, pt uint8) string {
	codec := "PCMU"
	if pt == 8 {
		codec = "PCMA"
	}
	session := time.Now().Unix()
	return fmt.Sprintf("v=0\r\no=talkgateway %d %d IN IP4 %s\r\ns=Nextcloud Talk Telephony Gateway\r\nc=IN IP4 %s\r\nt=0 0\r\nm=audio %d RTP/AVP %d 101\r\na=rtpmap:%d %s/8000\r\na=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16\r\na=ptime:20\r\na=sendrecv\r\n", session, session, ip, ip, port, pt, pt, codec)
}

func writeSIPResponse(conn *net.UDPConn, peer *net.UDPAddr, req sipRequest, code int, phrase, extra, body string) error {
	return writeSIPResponseWithTag(conn, peer, req, code, phrase, "", extra, body)
}
func writeSIPResponseWithTag(conn *net.UDPConn, peer *net.UDPAddr, req sipRequest, code int, phrase, toTag, extra, body string) error {
	to := req.header("to")
	if toTag != "" && !strings.Contains(strings.ToLower(to), ";tag=") {
		to += ";tag=" + toTag
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SIP/2.0 %d %s\r\n", code, phrase)
	for _, v := range req.headers["via"] {
		fmt.Fprintf(&b, "Via: %s\r\n", v)
	}
	fmt.Fprintf(&b, "From: %s\r\nTo: %s\r\nCall-ID: %s\r\nCSeq: %s\r\nServer: %s\r\n", req.header("from"), to, req.header("call-id"), req.header("cseq"), userAgent)
	if extra != "" {
		b.WriteString(extra)
	}
	fmt.Fprintf(&b, "Content-Length: %d\r\n\r\n%s", len(body), body)
	_, e := conn.WriteToUDP([]byte(b.String()), peer)
	return e
}

func sendInboundBYE(conn *net.UDPConn, peer *net.UDPAddr, invite sipRequest, toTag, contactURI string) error {
	target := extractSIPURI(invite.header("contact"))
	if target == "" {
		target = invite.header("from")
		target = extractSIPURI(target)
	}
	if target == "" {
		return errors.New("incoming dialog has no remote target")
	}
	host := localIPFor(peer)
	port := conn.LocalAddr().(*net.UDPAddr).Port
	branch := "z9hG4bK-" + newID("bye")
	from := "<" + contactURI + ">;tag=" + toTag
	to := invite.header("from")
	cid := invite.header("call-id")
	var b strings.Builder
	fmt.Fprintf(&b, "BYE %s SIP/2.0\r\nVia: SIP/2.0/UDP %s:%d;branch=%s;rport\r\nMax-Forwards: 70\r\nFrom: %s\r\nTo: %s\r\nCall-ID: %s\r\nCSeq: 2 BYE\r\nUser-Agent: %s\r\nContent-Length: 0\r\n\r\n", target, host, port, branch, from, to, cid, userAgent)
	log.Printf("Talk ended incoming call; sending SIP BYE")
	_, e := conn.WriteToUDP([]byte(b.String()), peer)
	return e
}

func sipUser(v string) string {
	v = extractSIPURI(v)
	i := strings.Index(strings.ToLower(v), "sip:")
	if i >= 0 {
		v = v[i+4:]
	}
	if j := strings.Index(v, "@"); j >= 0 {
		v = v[:j]
	}
	if j := strings.Index(v, ";"); j >= 0 {
		v = v[:j]
	}
	return strings.Trim(v, " <>\"")
}
func localIPFor(peer *net.UDPAddr) string {
	c, e := net.DialUDP("udp", nil, peer)
	if e == nil {
		defer c.Close()
		if a, ok := c.LocalAddr().(*net.UDPAddr); ok {
			return a.IP.String()
		}
	}
	return "127.0.0.1"
}
