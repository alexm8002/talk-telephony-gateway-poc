// SPDX-FileCopyrightText: 2026 2M Production Electrique
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"talk-telephony-gateway-poc/internal/simplews"
)

const userAgent = "nextcloud-talk-telephony-gateway-poc/0.9.1"

type config struct {
	HPBURL             string
	NextcloudURL       string
	InternalSecret     string
	RingDelay          time.Duration
	ConnectDelay       time.Duration
	ClearDelay         time.Duration
	Keepalive          time.Duration
	SIPServer          string
	SIPFromUser        string
	SIPDomain          string
	SIPLocalAddr       string
	SIPTimeout         time.Duration
	RTPLocalAddr       string
	RTPAdvertiseIP     string
	RTPTestDuration    time.Duration // legacy non-WebRTC test helper only
	SIPInboundListen   string
	SIPInboundTimeout  time.Duration
	SIPBridgeSecret    string
	AccountsAPIURL     string
	GatewayAPIToken    string
	AccountsRefresh    time.Duration
	SIPContactIP       string
	SIPRegisterExpires int
}

type clientMessage struct {
	ID       string                 `json:"id,omitempty"`
	Type     string                 `json:"type"`
	Hello    *helloClientMessage    `json:"hello,omitempty"`
	Internal *internalClientMessage `json:"internal,omitempty"`
	Room     *roomClientMessage     `json:"room,omitempty"`
}

type helloClientMessage struct {
	Version  string                  `json:"version"`
	Features []string                `json:"features,omitempty"`
	Auth     *helloClientMessageAuth `json:"auth"`
}

type helloClientMessageAuth struct {
	Type   string                 `json:"type"`
	Params internalAuthParameters `json:"params"`
}

type internalAuthParameters struct {
	Random  string `json:"random"`
	Token   string `json:"token"`
	Backend string `json:"backend"`
}

type serverMessage struct {
	ID       string                 `json:"id,omitempty"`
	Type     string                 `json:"type"`
	Error    *serverError           `json:"error,omitempty"`
	Hello    *helloServerMessage    `json:"hello,omitempty"`
	Internal *internalServerMessage `json:"internal,omitempty"`
	Room     *roomServerMessage     `json:"room,omitempty"`
	Event    *rtcServerEvent        `json:"event,omitempty"`
}

type serverError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type helloServerMessage struct {
	Version   string `json:"version,omitempty"`
	SessionID string `json:"sessionid,omitempty"`
	ResumeID  string `json:"resumeid,omitempty"`
	UserID    string `json:"userid,omitempty"`
}

type internalServerMessage struct {
	Type    string                        `json:"type"`
	Dialout *internalServerDialoutRequest `json:"dialout,omitempty"`
}

type internalServerDialoutRequest struct {
	RoomID  string                         `json:"roomid"`
	Backend string                         `json:"backend"`
	Request *internalServerDialoutContents `json:"request"`
}

type internalServerDialoutContents struct {
	Number  string          `json:"number"`
	Options json.RawMessage `json:"options,omitempty"`
}

type internalClientMessage struct {
	Type          string                        `json:"type"`
	Dialout       *dialoutInternalClientMessage `json:"dialout,omitempty"`
	AddSession    *addSessionMessage            `json:"addsession,omitempty"`
	InCall        *inCallMessage                `json:"incall,omitempty"`
	RemoveSession *removeSessionMessage         `json:"removesession,omitempty"`
}

type roomClientMessage struct {
	RoomID    string `json:"roomid"`
	SessionID string `json:"sessionid,omitempty"`
}

type roomServerMessage struct {
	RoomID string `json:"roomid"`
}

type addSessionOptions struct {
	ActorID   string `json:"actorId,omitempty"`
	ActorType string `json:"actorType,omitempty"`
}

type addSessionMessage struct {
	SessionID string             `json:"sessionid"`
	RoomID    string             `json:"roomid"`
	UserID    string             `json:"userid,omitempty"`
	User      map[string]any     `json:"user,omitempty"`
	InCall    *int               `json:"incall,omitempty"`
	Options   *addSessionOptions `json:"options,omitempty"`
}

type inCallMessage struct {
	InCall int `json:"incall"`
}

type removeSessionMessage struct {
	SessionID string `json:"sessionid"`
	RoomID    string `json:"roomid"`
	UserID    string `json:"userid,omitempty"`
}

type dialoutInternalClientMessage struct {
	Type   string                `json:"type"`
	RoomID string                `json:"roomid,omitempty"`
	Error  *serverError          `json:"error,omitempty"`
	Status *dialoutStatusMessage `json:"status,omitempty"`
}

type dialoutStatusMessage struct {
	CallID  string `json:"callid"`
	Status  string `json:"status"`
	Cause   string `json:"cause,omitempty"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type gateway struct {
	cfg          config
	conn         *simplews.Conn
	mu           sync.Mutex
	accounts     *accountManager
	sipTransport *sipTransport
	inboundReg   *inboundRegistry
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("starting %s", userAgent)
	log.Printf("HPB: %s", cfg.HPBURL)
	log.Printf("Nextcloud backend: %s", cfg.NextcloudURL)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	accounts, err := loadAccountManager(cfg)
	if err != nil {
		log.Fatal(err)
	}
	accounts.LogAccounts()
	g := &gateway{cfg: cfg, accounts: accounts}
	if err := g.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		HPBURL:             getenv("HPB_URL", "wss://talk.2mprodelec.fr/standalone-signaling/spreed"),
		NextcloudURL:       getenv("NEXTCLOUD_URL", "https://drive.2mprodelec.fr/"),
		RingDelay:          durationEnv("SIM_RING_DELAY", 1*time.Second),
		ConnectDelay:       durationEnv("SIM_CONNECT_DELAY", 2*time.Second),
		ClearDelay:         durationEnv("SIM_CLEAR_DELAY", 8*time.Second),
		Keepalive:          durationEnv("WS_KEEPALIVE", 15*time.Second),
		SIPServer:          strings.TrimSpace(os.Getenv("SIP_SERVER")),
		SIPFromUser:        getenv("SIP_FROM_USER", "talk"),
		SIPDomain:          strings.TrimSpace(os.Getenv("SIP_DOMAIN")),
		SIPLocalAddr:       getenv("SIP_LOCAL_ADDR", "0.0.0.0:0"),
		SIPTimeout:         durationEnv("SIP_TIMEOUT", 45*time.Second),
		RTPLocalAddr:       getenv("RTP_LOCAL_ADDR", "0.0.0.0:0"),
		RTPAdvertiseIP:     strings.TrimSpace(os.Getenv("RTP_ADVERTISE_IP")),
		RTPTestDuration:    durationEnv("RTP_TEST_DURATION", 20*time.Second),
		SIPInboundListen:   strings.TrimSpace(os.Getenv("SIP_INBOUND_LISTEN")),
		SIPInboundTimeout:  durationEnv("SIP_INBOUND_TIMEOUT", 60*time.Second),
		SIPBridgeSecret:    os.Getenv("SPREED_SIP_SECRET"),
		AccountsAPIURL:     strings.TrimSpace(os.Getenv("NEXTCLOUD_ACCOUNTS_URL")),
		GatewayAPIToken:    os.Getenv("NEXTCLOUD_GATEWAY_TOKEN"),
		AccountsRefresh:    durationEnv("NEXTCLOUD_ACCOUNTS_REFRESH", 30*time.Second),
		SIPContactIP:       strings.TrimSpace(os.Getenv("SIP_CONTACT_IP")),
		SIPRegisterExpires: intEnv("SIP_REGISTER_EXPIRES", 300),
	}
	cfg.InternalSecret = os.Getenv("HPB_INTERNAL_SECRET")
	if cfg.InternalSecret == "" {
		return cfg, errors.New("HPB_INTERNAL_SECRET is required")
	}
	if !strings.HasSuffix(cfg.NextcloudURL, "/") {
		cfg.NextcloudURL += "/"
	}
	if cfg.AccountsAPIURL == "" {
		cfg.AccountsAPIURL = strings.TrimRight(cfg.NextcloudURL, "/") + "/index.php/apps/talk_telephony/api/v1/gateway/accounts"
	}
	if cfg.GatewayAPIToken == "" {
		return cfg, errors.New("NEXTCLOUD_GATEWAY_TOKEN is required")
	}
	if cfg.AccountsRefresh < 5*time.Second {
		return cfg, errors.New("NEXTCLOUD_ACCOUNTS_REFRESH must be at least 5s")
	}
	if !strings.HasPrefix(cfg.HPBURL, "ws://") && !strings.HasPrefix(cfg.HPBURL, "wss://") {
		return cfg, fmt.Errorf("HPB_URL must start with ws:// or wss://: %s", cfg.HPBURL)
	}
	if cfg.SIPServer == "" {
		return cfg, errors.New("SIP_SERVER is required")
	}
	if cfg.SIPInboundListen != "" && cfg.SIPBridgeSecret == "" {
		return cfg, errors.New("SPREED_SIP_SECRET is required when inbound SIP is enabled")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func inboundListenEnv() string {
	v := strings.TrimSpace(os.Getenv("SIP_INBOUND_LISTEN"))
	if strings.EqualFold(v, "off") || strings.EqualFold(v, "disabled") {
		return ""
	}
	if v == "" {
		return "0.0.0.0:5060"
	}
	return v
}

func intEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		log.Fatalf("invalid %s=%q", key, v)
	}
	return n
}

func durationEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func (g *gateway) run(ctx context.Context) error {
	if g.cfg.SIPInboundListen != "" {
		t, err := newSIPTransport(g.cfg.SIPInboundListen, g.cfg.SIPServer)
		if err != nil {
			return fmt.Errorf("create SIP transport: %w", err)
		}
		g.sipTransport = t
		g.inboundReg = &inboundRegistry{calls: make(map[string]*inboundCallControl)}
		t.SetRequestHandler(g.handleSharedSIPRequest)
		g.accounts.SetTransport(t)
		log.Printf("SIP shared transport ready: %s", t.LocalAddr())
		go t.run(ctx)
	}
	if g.accounts != nil {
		g.accounts.RunRegistrations(ctx)
	}
	conn, resp, err := simplews.Dial(ctx, g.cfg.HPBURL, userAgent)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("connect HPB: %w (HTTP %s)", err, resp.Status)
		}
		return fmt.Errorf("connect HPB: %w", err)
	}
	defer conn.Close()
	g.conn = conn

	// ReadJSON blocks on the socket. Close it as soon as SIGINT/SIGTERM fires
	// so Ctrl+C interrupts the blocking read immediately.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	log.Printf("websocket connected")

	if err := g.sendHello(); err != nil {
		return err
	}

	keepaliveDone := make(chan struct{})
	defer close(keepaliveDone)
	go g.keepaliveLoop(keepaliveDone)

	helloOK := false
	for {
		select {
		case <-ctx.Done():
			_ = conn.Close()
			return ctx.Err()
		default:
		}

		var msg serverMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read websocket: %w", err)
		}

		switch msg.Type {
		case "hello":
			helloOK = true
			if msg.Hello != nil {
				log.Printf("HPB hello accepted: session=%s version=%s", msg.Hello.SessionID, msg.Hello.Version)
			} else {
				log.Printf("HPB hello accepted")
			}
		case "error":
			if msg.Error != nil {
				return fmt.Errorf("HPB error: code=%s message=%s", msg.Error.Code, msg.Error.Message)
			}
			return errors.New("HPB returned an unspecified error")
		case "internal":
			if !helloOK {
				log.Printf("warning: received internal message before hello response")
			}
			if msg.Internal != nil && msg.Internal.Type == "dialout" && msg.Internal.Dialout != nil {
				go g.handleDialout(msg.ID, msg.Internal.Dialout)
			} else {
				log.Printf("ignoring unsupported internal message: %+v", msg.Internal)
			}
		default:
			log.Printf("received %s message", msg.Type)
		}
	}
}

func (g *gateway) keepaliveLoop(done <-chan struct{}) {
	if g.cfg.Keepalive <= 0 {
		return
	}
	ticker := time.NewTicker(g.cfg.Keepalive)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			payload := []byte(fmt.Sprintf("%d", t.UnixNano()))
			if err := g.conn.WritePing(payload); err != nil {
				log.Printf("websocket keepalive ping failed: %v", err)
				return
			}
			log.Printf("websocket keepalive ping sent")
		}
	}
}

func (g *gateway) sendHello() error {
	rnd, err := randomHex(48)
	if err != nil {
		return fmt.Errorf("generate auth random: %w", err)
	}

	mac := hmac.New(sha256.New, []byte(g.cfg.InternalSecret))
	_, _ = mac.Write([]byte(rnd))
	token := hex.EncodeToString(mac.Sum(nil))

	msg := clientMessage{
		ID:   newID("hello"),
		Type: "hello",
		Hello: &helloClientMessage{
			Version:  "1.0",
			Features: []string{"start-dialout"},
			Auth: &helloClientMessageAuth{
				Type: "internal",
				Params: internalAuthParameters{
					Random:  rnd,
					Token:   token,
					Backend: g.cfg.NextcloudURL,
				},
			},
		},
	}

	log.Printf("sending internal hello with feature start-dialout")
	return g.writeJSON(msg)
}

func (g *gateway) handleDialout(requestID string, req *internalServerDialoutRequest) {
	if req.Request == nil {
		log.Printf("dialout request without request payload, id=%s", requestID)
		return
	}

	callID := newID("sip-call")
	log.Printf("DIALOUT received: request=%s room=%s backend=%s number=%s options=%s callid=%s",
		requestID, req.RoomID, req.Backend, req.Request.Number, compactJSON(req.Request.Options), callID)

	if err := g.sendDialoutStatus(requestID, req.RoomID, callID, "accepted", "", 0, ""); err != nil {
		log.Printf("send accepted: %v", err)
		return
	}

	if g.cfg.SIPServer == "" {
		log.Printf("SIP_SERVER not configured; keeping simulation mode")
		time.Sleep(g.cfg.RingDelay)
		_ = g.sendDialoutStatus(newID("status"), req.RoomID, callID, "ringing", "", 0, "")
		time.Sleep(g.cfg.ConnectDelay)
		_ = g.sendDialoutStatus(newID("status"), req.RoomID, callID, "connected", "", 0, "")
		time.Sleep(g.cfg.ClearDelay)
		_ = g.sendDialoutStatus(newID("status"), req.RoomID, callID, "cleared", "poc-complete", 200, "POC simulated call completed")
		return
	}

	caller := g.cfg.SIPFromUser
	var opts struct {
		Caller    string `json:"caller"`
		ActorID   string `json:"actorId"`
		ActorType string `json:"actorType"`
	}
	if len(req.Request.Options) > 0 && json.Unmarshal(req.Request.Options, &opts) == nil && strings.TrimSpace(opts.Caller) != "" {
		caller = strings.TrimSpace(opts.Caller)
	}

	callSession, err := g.openTalkCallSession(req.RoomID, callID, req.Request.Number, opts.ActorID, opts.ActorType)
	if err != nil {
		log.Printf("Talk call session setup failed: callid=%s error=%v", callID, err)
		_ = g.sendDialoutStatus(newID("status"), req.RoomID, callID, "cleared", "talk-session-error", 500, err.Error())
		return
	}
	defer callSession.Close()

	nextcloudUser := callSession.PrimaryNextcloudUser()
	account, err := g.accounts.SelectOutbound(caller, nextcloudUser)
	if err != nil {
		log.Printf("SIP account selection failed: callid=%s nextcloud_user=%s caller=%s error=%v", callID, nextcloudUser, caller, err)
		_ = g.sendDialoutStatus(newID("status"), req.RoomID, callID, "cleared", "account-selection-error", 500, err.Error())
		return
	}
	log.Printf("SIP outbound account selected: extension=%s nextcloud_user=%s caller=%s", account.Extension, account.NextcloudUser, caller)

	if err := g.sipDial(req.RoomID, callID, caller, req.Request.Number, account, callSession); err != nil {
		log.Printf("SIP call failed: callid=%s error=%v", callID, err)
		_ = g.sendDialoutStatus(newID("status"), req.RoomID, callID, "cleared", "sip-error", 500, err.Error())
	}
}

type talkCallSession struct {
	conn            *simplews.Conn
	sessionID       string
	roomID          string
	virtualID       string
	keepalive       time.Duration
	initialSessions []rtcRoomSession
	closeOnce       sync.Once
}

func (s *talkCallSession) PrimaryNextcloudUser() string {
	for _, p := range s.initialSessions {
		if p.UserID == "" || p.SessionID == s.sessionID || p.SessionID == s.virtualID {
			continue
		}
		if t, _ := p.User["type"].(string); t == "phone" {
			continue
		}
		return p.UserID
	}
	return ""
}

func (s *talkCallSession) Close() {
	s.closeOnce.Do(func() {
		if s.conn == nil {
			return
		}
		remove := clientMessage{
			ID:   newID("remove"),
			Type: "internal",
			Internal: &internalClientMessage{
				Type: "removesession",
				RemoveSession: &removeSessionMessage{
					SessionID: s.virtualID,
					RoomID:    s.roomID,
				},
			},
		}
		_ = s.conn.WriteJSON(remove)
		log.Printf("HPB virtual phone session remove requested: room=%s virtual=%s", s.roomID, s.virtualID)
		_ = s.conn.Close()
	})
}

func (g *gateway) openTalkCallSession(roomID, callID, number, actorID, actorType string) (*talkCallSession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, resp, err := simplews.Dial(ctx, g.cfg.HPBURL, userAgent)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("connect HPB call session: %w (HTTP %s)", err, resp.Status)
		}
		return nil, fmt.Errorf("connect HPB call session: %w", err)
	}

	s := &talkCallSession{conn: conn, roomID: roomID, virtualID: callID, keepalive: g.cfg.Keepalive}
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()

	// Welcome is sent immediately on connect. Drain it before hello.
	var welcome serverMessage
	if err := conn.ReadJSON(&welcome); err != nil {
		return nil, fmt.Errorf("read HPB call-session welcome: %w", err)
	}

	rnd, err := randomHex(48)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, []byte(g.cfg.InternalSecret))
	_, _ = mac.Write([]byte(rnd))
	helloID := newID("call-hello")
	hello := clientMessage{
		ID:   helloID,
		Type: "hello",
		Hello: &helloClientMessage{
			Version: "1.0",
			Auth: &helloClientMessageAuth{
				Type:   "internal",
				Params: internalAuthParameters{Random: rnd, Token: hex.EncodeToString(mac.Sum(nil)), Backend: g.cfg.NextcloudURL},
			},
		},
	}
	if err := conn.WriteJSON(hello); err != nil {
		return nil, fmt.Errorf("send HPB call-session hello: %w", err)
	}
	for {
		var msg serverMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return nil, fmt.Errorf("read HPB call-session hello: %w", err)
		}
		if msg.Type == "error" {
			return nil, fmt.Errorf("HPB call-session hello rejected: %+v", msg.Error)
		}
		if msg.Type == "hello" && msg.ID == helloID {
			if msg.Hello != nil {
				s.sessionID = msg.Hello.SessionID
				log.Printf("HPB call internal session accepted: session=%s", msg.Hello.SessionID)
			}
			break
		}
	}

	joinID := newID("room")
	join := clientMessage{ID: joinID, Type: "room", Room: &roomClientMessage{RoomID: roomID}}
	if err := conn.WriteJSON(join); err != nil {
		return nil, fmt.Errorf("join HPB room: %w", err)
	}
	for {
		var msg serverMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return nil, fmt.Errorf("read HPB room join: %w", err)
		}
		if msg.Type == "event" && msg.Event != nil && msg.Event.Target == "room" && msg.Event.Type == "join" {
			s.initialSessions = append(s.initialSessions, msg.Event.Join...)
			continue
		}
		if msg.ID != joinID {
			continue
		}
		if msg.Type == "error" {
			return nil, fmt.Errorf("HPB room join rejected: %+v", msg.Error)
		}
		if msg.Type == "room" && msg.Room != nil && msg.Room.RoomID == roomID {
			log.Printf("HPB call internal session joined room=%s", roomID)
			break
		}
	}

	// The real internal gateway session is the MCU audio publisher.
	// 1 = in-call, 2 = with-audio, therefore 3 = in-call | with-audio.
	gatewayInCall := clientMessage{
		ID:   newID("incall"),
		Type: "internal",
		Internal: &internalClientMessage{
			Type: "incall",
			InCall: &inCallMessage{
				InCall: 3,
			},
		},
	}
	if err := conn.WriteJSON(gatewayInCall); err != nil {
		return nil, fmt.Errorf("set HPB gateway session in-call state: %w", err)
	}
	log.Printf("HPB call internal session marked in-call with audio: session=%s room=%s incall=3", s.sessionID, roomID)

	phoneInCall := 9 // 1 = in-call, 8 = with-phone.
	add := clientMessage{
		ID:   newID("add"),
		Type: "internal",
		Internal: &internalClientMessage{
			Type: "addsession",
			AddSession: &addSessionMessage{
				SessionID: callID,
				RoomID:    roomID,
				InCall:    &phoneInCall,
				User: map[string]any{
					"type":   "phone",
					"callid": callID,
					"number": number,
				},
			},
		},
	}
	if actorID != "" && actorType != "" {
		add.Internal.AddSession.Options = &addSessionOptions{ActorID: actorID, ActorType: actorType}
	}
	if err := conn.WriteJSON(add); err != nil {
		return nil, fmt.Errorf("add HPB virtual phone session: %w", err)
	}
	log.Printf("HPB virtual phone session added: room=%s virtual=%s number=%s actor=%s/%s incall=9", roomID, callID, number, actorType, actorID)

	// Keep this per-call internal websocket active while SIP/RTP is alive. The
	// signaling server pings less often than some reverse-proxy idle timeouts.
	if s.keepalive > 0 {
		go func() {
			t := time.NewTicker(s.keepalive)
			defer t.Stop()
			for range t.C {
				if err := conn.WritePing([]byte(strconv.FormatInt(time.Now().UnixNano(), 10))); err != nil {
					return
				}
			}
		}()
	}

	ok = true
	return s, nil
}

func (g *gateway) sipDial(roomID, callID, caller, number string, account *sipAccount, callSession *talkCallSession) error {
	server := g.cfg.SIPServer
	if !strings.Contains(server, ":") {
		server += ":5060"
	}
	raddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return fmt.Errorf("resolve SIP server: %w", err)
	}
	laddr, err := net.ResolveUDPAddr("udp", g.cfg.SIPLocalAddr)
	if err != nil {
		return fmt.Errorf("resolve SIP local address: %w", err)
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return fmt.Errorf("open SIP UDP socket: %w", err)
	}
	defer conn.Close()

	local := conn.LocalAddr().(*net.UDPAddr)
	domain := g.cfg.SIPDomain
	if domain == "" {
		domain = raddr.IP.String()
	}
	contactHost := local.IP.String()
	if contactHost == "0.0.0.0" || contactHost == "::" {
		if probe, err := net.DialUDP("udp", nil, raddr); err == nil {
			if a, ok := probe.LocalAddr().(*net.UDPAddr); ok {
				contactHost = a.IP.String()
			}
			_ = probe.Close()
		}
	}
	if contactHost == "0.0.0.0" || contactHost == "::" || contactHost == "" {
		contactHost = "127.0.0.1"
	}

	// Allocate the RTP socket before creating the SDP offer so the advertised
	// port is the real local UDP port that will receive media from Asterisk.
	rtpLAddr, err := net.ResolveUDPAddr("udp", g.cfg.RTPLocalAddr)
	if err != nil {
		return fmt.Errorf("resolve RTP local address: %w", err)
	}
	rtpConn, err := net.ListenUDP("udp", rtpLAddr)
	if err != nil {
		return fmt.Errorf("open RTP UDP socket: %w", err)
	}
	defer rtpConn.Close()
	rtpLocal := rtpConn.LocalAddr().(*net.UDPAddr)
	rtpAdvertiseIP := g.cfg.RTPAdvertiseIP
	if rtpAdvertiseIP == "" {
		rtpAdvertiseIP = contactHost
	}
	sdpOffer := buildSDPOffer(rtpAdvertiseIP, rtpLocal.Port)
	log.Printf("RTP local socket ready: listen=%s advertise=%s:%d codecs=PCMA/PCMU", rtpLocal, rtpAdvertiseIP, rtpLocal.Port)

	tag := newID("t")
	sipCallID := callID + "@talk-gateway"
	uri := "sip:" + number + "@" + domain
	fromUser := account.Username
	fromURI := "sip:" + fromUser + "@" + domain
	contactURI := fmt.Sprintf("sip:%s@%s:%d", g.cfg.SIPFromUser, contactHost, local.Port)
	cseq := 1

	buildInvite := func(branch, authHeader string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "INVITE %s SIP/2.0\r\n", uri)
		fmt.Fprintf(&b, "Via: SIP/2.0/UDP %s:%d;branch=%s;rport\r\n", contactHost, local.Port, branch)
		fmt.Fprintf(&b, "Max-Forwards: 70\r\n")
		fmt.Fprintf(&b, "From: <%s>;tag=%s\r\n", fromURI, tag)
		fmt.Fprintf(&b, "To: <%s>\r\n", uri)
		fmt.Fprintf(&b, "Call-ID: %s\r\n", sipCallID)
		fmt.Fprintf(&b, "CSeq: %d INVITE\r\n", cseq)
		fmt.Fprintf(&b, "Contact: <%s>\r\n", contactURI)
		fmt.Fprintf(&b, "User-Agent: %s\r\n", userAgent)
		fmt.Fprintf(&b, "Allow: INVITE, ACK, CANCEL, BYE, OPTIONS, INFO\r\n")
		fmt.Fprintf(&b, "Supported: timer\r\n")
		if authHeader != "" {
			fmt.Fprintf(&b, "%s\r\n", authHeader)
		}
		fmt.Fprintf(&b, "Content-Type: application/sdp\r\n")
		fmt.Fprintf(&b, "Content-Length: %d\r\n\r\n%s", len(sdpOffer), sdpOffer)
		return b.String()
	}

	buildACK := func(resp sipResponse, branch string) string {
		to := resp.header("to")
		if to == "" {
			to = "<" + uri + ">"
		}
		target := uri
		if c := extractSIPURI(resp.header("contact")); c != "" && resp.code >= 200 && resp.code < 300 {
			target = c
		}
		var b strings.Builder
		fmt.Fprintf(&b, "ACK %s SIP/2.0\r\n", target)
		fmt.Fprintf(&b, "Via: SIP/2.0/UDP %s:%d;branch=%s;rport\r\n", contactHost, local.Port, branch)
		fmt.Fprintf(&b, "Max-Forwards: 70\r\n")
		fmt.Fprintf(&b, "From: <%s>;tag=%s\r\n", fromURI, tag)
		fmt.Fprintf(&b, "To: %s\r\n", to)
		fmt.Fprintf(&b, "Call-ID: %s\r\n", sipCallID)
		fmt.Fprintf(&b, "CSeq: %d ACK\r\n", cseq)
		fmt.Fprintf(&b, "Content-Length: 0\r\n\r\n")
		return b.String()
	}

	branch := "z9hG4bK-" + newID("b")
	invite := buildInvite(branch, "")
	log.Printf("SIP INVITE: server=%s auth-from=%s caller=%s to=%s with SDP", server, fromUser, caller, number)
	if _, err := conn.WriteToUDP([]byte(invite), raddr); err != nil {
		return fmt.Errorf("send INVITE: %w", err)
	}

	deadline := time.Now().Add(g.cfg.SIPTimeout)
	buf := make([]byte, 65535)
	ringingSent := false
	authTried := false

	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := conn.ReadFromUDP(buf)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			continue
		}
		if err != nil {
			return fmt.Errorf("read SIP response: %w", err)
		}
		resp, err := parseSIPResponse(string(buf[:n]))
		if err != nil {
			continue
		}
		log.Printf("SIP response: %s", resp.statusLine)

		switch {
		case resp.code == 180 || resp.code == 183:
			if !ringingSent {
				ringingSent = true
				_ = g.sendDialoutStatus(newID("status"), roomID, callID, "ringing", "", resp.code, resp.statusLine)
			}

		case resp.code == 401 || resp.code == 407:
			ack := buildACK(resp, branch)
			_, _ = conn.WriteToUDP([]byte(ack), raddr)
			if authTried {
				return fmt.Errorf("SIP authentication rejected after retry: %s", resp.statusLine)
			}
			if account.AuthUser == "" || account.Password == "" {
				return fmt.Errorf("SIP authentication required (%d) but account %s has no credentials", resp.code, account.Extension)
			}
			headerName := "www-authenticate"
			authorizationName := "Authorization"
			if resp.code == 407 {
				headerName = "proxy-authenticate"
				authorizationName = "Proxy-Authorization"
			}
			challengeRaw := resp.header(headerName)
			if challengeRaw == "" {
				return fmt.Errorf("SIP %d response has no authentication challenge", resp.code)
			}
			challenge, err := parseDigestChallenge(challengeRaw)
			if err != nil {
				return fmt.Errorf("parse SIP digest challenge: %w", err)
			}
			authValue, err := buildDigestAuthorization(challenge, account.AuthUser, account.Password, "INVITE", uri)
			if err != nil {
				return fmt.Errorf("build SIP digest authorization: %w", err)
			}
			authTried = true
			cseq++
			branch = "z9hG4bK-" + newID("b")
			invite = buildInvite(branch, authorizationName+": "+authValue)
			log.Printf("SIP authentication challenge accepted: realm=%s algorithm=%s qop=%s; retrying INVITE as %s", challenge.realm, challenge.algorithm, challenge.qop, account.AuthUser)
			if _, err := conn.WriteToUDP([]byte(invite), raddr); err != nil {
				return fmt.Errorf("send authenticated INVITE: %w", err)
			}

		case resp.code >= 200 && resp.code < 300:
			ack := buildACK(resp, "z9hG4bK-"+newID("ack"))
			_, _ = conn.WriteToUDP([]byte(ack), raddr)
			_ = g.sendDialoutStatus(newID("status"), roomID, callID, "connected", "", resp.code, resp.statusLine)

			remoteRTP, payloadType, codec, err := parseSDPAnswer(resp.body)
			if err != nil {
				return fmt.Errorf("SIP 200 OK did not contain usable audio SDP: %w", err)
			}
			log.Printf("SDP negotiated: remote RTP=%s codec=%s payload=%d", remoteRTP, codec, payloadType)

			remoteTarget := extractSIPURI(resp.header("contact"))
			if remoteTarget == "" {
				remoteTarget = uri
			}
			if err := g.runRTPWebRTCBridge(conn, raddr, rtpConn, remoteRTP, payloadType, codec, roomID, callID, fromURI, tag, resp.header("to"), sipCallID, &cseq, contactHost, local.Port, remoteTarget, callSession); err != nil {
				return err
			}
			_ = g.sendDialoutStatus(newID("status"), roomID, callID, "cleared", "normal-clearing", 200, "SIP/WebRTC bridge completed")
			return nil

		case resp.code >= 300:
			ack := buildACK(resp, branch)
			_, _ = conn.WriteToUDP([]byte(ack), raddr)
			return fmt.Errorf("SIP rejected call: %s", resp.statusLine)
		}
	}
	return fmt.Errorf("SIP INVITE timed out after %s", g.cfg.SIPTimeout)
}

func buildSDPOffer(ip string, port int) string {
	session := time.Now().Unix()
	return fmt.Sprintf("v=0\r\no=talkgateway %d %d IN IP4 %s\r\ns=Nextcloud Talk Telephony Gateway\r\nc=IN IP4 %s\r\nt=0 0\r\nm=audio %d RTP/AVP 8 0 101\r\na=rtpmap:8 PCMA/8000\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:101 telephone-event/8000\r\na=fmtp:101 0-16\r\na=ptime:20\r\na=sendrecv\r\n", session, session, ip, ip, port)
}

func parseSDPAnswer(body string) (*net.UDPAddr, uint8, string, error) {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	var connIP string
	var port int
	var payloads []int
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "c=IN IP4 ") {
			connIP = strings.TrimSpace(strings.TrimPrefix(line, "c=IN IP4 "))
		}
		if strings.HasPrefix(line, "m=audio ") {
			f := strings.Fields(line)
			if len(f) >= 4 {
				port, _ = strconv.Atoi(f[1])
				for _, p := range f[3:] {
					if n, e := strconv.Atoi(p); e == nil {
						payloads = append(payloads, n)
					}
				}
			}
		}
	}
	if connIP == "" || port <= 0 {
		return nil, 0, "", errors.New("missing c= address or m=audio port")
	}
	pt := -1
	codec := ""
	for _, p := range payloads {
		if p == 8 {
			pt, codec = 8, "PCMA"
			break
		}
		if p == 0 && pt < 0 {
			pt, codec = 0, "PCMU"
		}
	}
	if pt < 0 {
		return nil, 0, "", fmt.Errorf("no supported G.711 payload in %v", payloads)
	}
	a, err := net.ResolveUDPAddr("udp", net.JoinHostPort(connIP, strconv.Itoa(port)))
	if err != nil {
		return nil, 0, "", err
	}
	return a, uint8(pt), codec, nil
}

func extractSIPURI(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if i := strings.Index(v, "<"); i >= 0 {
		if j := strings.Index(v[i+1:], ">"); j >= 0 {
			return strings.TrimSpace(v[i+1 : i+1+j])
		}
	}
	if i := strings.Index(v, ";"); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v)
}

func (g *gateway) runRTPTest(sipConn *net.UDPConn, sipServer *net.UDPAddr, rtpConn *net.UDPConn, remoteRTP *net.UDPAddr, payloadType uint8, codec, roomID, callID, fromURI, tag, toHeader, sipCallID string, cseq *int, contactHost string, sipPort int, remoteTarget string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var rxPackets atomic.Uint64
	var rxBytes atomic.Uint64
	var txPackets atomic.Uint64
	var txBytes atomic.Uint64

	go func() {
		buf := make([]byte, 2048)
		first := true
		for {
			_ = rtpConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, from, err := rtpConn.ReadFromUDP(buf)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			if err != nil {
				return
			}
			if n < 12 {
				continue
			}
			count := rxPackets.Add(1)
			rxBytes.Add(uint64(n))
			if first {
				first = false
				log.Printf("RTP first packet received: from=%s bytes=%d pt=%d", from, n, buf[1]&0x7f)
			}
			if count%250 == 0 {
				log.Printf("RTP receive progress: packets=%d bytes=%d", count, rxBytes.Load())
			}
		}
	}()

	go func() {
		seq := randomUint16()
		ts := randomUint32()
		ssrc := randomUint32()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		first := true
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pkt := make([]byte, 12+160)
				pkt[0] = 0x80
				pkt[1] = payloadType
				if first {
					pkt[1] |= 0x80
					first = false
				}
				binary.BigEndian.PutUint16(pkt[2:4], seq)
				binary.BigEndian.PutUint32(pkt[4:8], ts)
				binary.BigEndian.PutUint32(pkt[8:12], ssrc)
				silence := byte(0xff)
				if payloadType == 8 {
					silence = 0xd5
				}
				for i := 12; i < len(pkt); i++ {
					pkt[i] = silence
				}
				n, err := rtpConn.WriteToUDP(pkt, remoteRTP)
				if err != nil {
					log.Printf("RTP send error: %v", err)
					return
				}
				txPackets.Add(1)
				txBytes.Add(uint64(n))
				seq++
				ts += 160
			}
		}
	}()

	log.Printf("RTP test active for %s: sending %s silence to %s and listening for return media", g.cfg.RTPTestDuration, codec, remoteRTP)
	end := time.Now().Add(g.cfg.RTPTestDuration)
	buf := make([]byte, 65535)
	remoteHungUp := false
	for time.Now().Before(end) {
		_ = sipConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, peer, err := sipConn.ReadFromUDP(buf)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			continue
		}
		if err != nil {
			break
		}
		raw := string(buf[:n])
		if strings.HasPrefix(raw, "BYE ") {
			log.Printf("SIP remote BYE received from %s", peer)
			resp := buildSimpleSIP200(raw)
			_, _ = sipConn.WriteToUDP([]byte(resp), peer)
			remoteHungUp = true
			break
		}
	}
	cancel()
	time.Sleep(50 * time.Millisecond)
	log.Printf("RTP test summary: codec=%s rx_packets=%d rx_bytes=%d tx_packets=%d tx_bytes=%d", codec, rxPackets.Load(), rxBytes.Load(), txPackets.Load(), txBytes.Load())

	if remoteHungUp {
		return nil
	}

	*cseq++
	branch := "z9hG4bK-" + newID("bye")
	var b strings.Builder
	fmt.Fprintf(&b, "BYE %s SIP/2.0\r\n", remoteTarget)
	fmt.Fprintf(&b, "Via: SIP/2.0/UDP %s:%d;branch=%s;rport\r\n", contactHost, sipPort, branch)
	fmt.Fprintf(&b, "Max-Forwards: 70\r\n")
	fmt.Fprintf(&b, "From: <%s>;tag=%s\r\n", fromURI, tag)
	if toHeader == "" {
		toHeader = "<" + remoteTarget + ">"
	}
	fmt.Fprintf(&b, "To: %s\r\n", toHeader)
	fmt.Fprintf(&b, "Call-ID: %s\r\n", sipCallID)
	fmt.Fprintf(&b, "CSeq: %d BYE\r\n", *cseq)
	fmt.Fprintf(&b, "User-Agent: %s\r\n", userAgent)
	fmt.Fprintf(&b, "Content-Length: 0\r\n\r\n")
	log.Printf("SIP test duration elapsed; sending BYE")
	_, err := sipConn.WriteToUDP([]byte(b.String()), sipServer)
	if err != nil {
		return fmt.Errorf("send BYE: %w", err)
	}
	_ = sipConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, _, e := sipConn.ReadFromUDP(buf); e == nil {
		if resp, e2 := parseSIPResponse(string(buf[:n])); e2 == nil {
			log.Printf("SIP BYE response: %s", resp.statusLine)
		}
	}
	return nil
}

func sendSIPBye(sipConn *net.UDPConn, sipServer *net.UDPAddr, remoteTarget, fromURI, tag, toHeader, sipCallID string, cseq *int, contactHost string, sipPort int) error {
	*cseq++
	branch := "z9hG4bK-" + newID("bye")
	var b strings.Builder
	fmt.Fprintf(&b, "BYE %s SIP/2.0\r\n", remoteTarget)
	fmt.Fprintf(&b, "Via: SIP/2.0/UDP %s:%d;branch=%s;rport\r\n", contactHost, sipPort, branch)
	fmt.Fprintf(&b, "Max-Forwards: 70\r\n")
	fmt.Fprintf(&b, "From: <%s>;tag=%s\r\n", fromURI, tag)
	if toHeader == "" {
		toHeader = "<" + remoteTarget + ">"
	}
	fmt.Fprintf(&b, "To: %s\r\n", toHeader)
	fmt.Fprintf(&b, "Call-ID: %s\r\n", sipCallID)
	fmt.Fprintf(&b, "CSeq: %d BYE\r\n", *cseq)
	fmt.Fprintf(&b, "User-Agent: %s\r\n", userAgent)
	fmt.Fprintf(&b, "Content-Length: 0\r\n\r\n")
	log.Printf("Talk call ended; sending SIP BYE")
	if _, err := sipConn.WriteToUDP([]byte(b.String()), sipServer); err != nil {
		return fmt.Errorf("send BYE: %w", err)
	}
	buf := make([]byte, 65535)
	_ = sipConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, _, err := sipConn.ReadFromUDP(buf); err == nil {
		if resp, err := parseSIPResponse(string(buf[:n])); err == nil {
			log.Printf("SIP BYE response: %s", resp.statusLine)
		}
	}
	return nil
}

func (g *gateway) runRTPWebRTCBridge(sipConn *net.UDPConn, sipServer *net.UDPAddr, rtpConn *net.UDPConn, remoteRTP *net.UDPAddr, payloadType uint8, codec, roomID, callID, fromURI, tag, toHeader, sipCallID string, cseq *int, contactHost string, sipPort int, remoteTarget string, callSession *talkCallSession) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for one real SIP RTP packet before creating the WebRTC publisher.
	// Asterisk may actually send PT0 although its SDP selected PT8, so the
	// packet on the wire is authoritative for the SIP->Talk direction.
	first := make([]byte, 4096)
	_ = rtpConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, from, err := rtpConn.ReadFromUDP(first)
	if err != nil {
		return fmt.Errorf("wait for first SIP RTP packet: %w", err)
	}
	first = append([]byte(nil), first[:n]...)
	actualPT, err := firstRTPPayloadType(first)
	if err != nil {
		return err
	}
	if actualPT != 0 && actualPT != 8 {
		return fmt.Errorf("unsupported live SIP RTP payload %d from %s", actualPT, from)
	}
	log.Printf("SIP RTP live payload detected: from=%s pt=%d codec=%s (SDP selected pt=%d/%s)", from, actualPT, codecName(actualPT), payloadType, codec)

	bridge, err := callSession.StartMediaBridge(ctx, actualPT, rtpConn, remoteRTP, payloadType)
	if err != nil {
		return fmt.Errorf("start WebRTC media bridge: %w", err)
	}
	defer bridge.Close()
	if err := bridge.PublishSIPRTP(first); err != nil {
		return fmt.Errorf("publish first SIP RTP packet to WebRTC: %w", err)
	}

	rtpDone := make(chan struct{})
	go func() {
		defer close(rtpDone)
		buf := make([]byte, 4096)
		for {
			_ = rtpConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, _, err := rtpConn.ReadFromUDP(buf)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			if err != nil {
				return
			}
			if err := bridge.PublishSIPRTP(buf[:n]); err != nil {
				log.Printf("SIP->WebRTC publish error: %v", err)
				return
			}
		}
	}()

	log.Printf("SIP/WebRTC live bridge active: SIP %s <-> HPB/Janus room=%s", remoteRTP, roomID)
	buf := make([]byte, 65535)
	for {
		_ = sipConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, peer, err := sipConn.ReadFromUDP(buf)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			select {
			case <-bridge.TalkEnded():
				cancel()
				select {
				case <-rtpDone:
				case <-time.After(500 * time.Millisecond):
				}
				return sendSIPBye(sipConn, sipServer, remoteTarget, fromURI, tag, toHeader, sipCallID, cseq, contactHost, sipPort)
			case <-ctx.Done():
				cancel()
				return ctx.Err()
			default:
				continue
			}
		}
		if err != nil {
			cancel()
			return err
		}
		raw := string(buf[:n])
		switch {
		case strings.HasPrefix(raw, "BYE "):
			log.Printf("SIP remote BYE received from %s", peer)
			resp := buildSimpleSIP200(raw)
			_, _ = sipConn.WriteToUDP([]byte(resp), peer)
			cancel()
			select {
			case <-rtpDone:
			case <-time.After(500 * time.Millisecond):
			}
			return nil
		case strings.HasPrefix(raw, "CANCEL "):
			log.Printf("SIP remote CANCEL received from %s", peer)
			resp := buildSimpleSIP200(raw)
			_, _ = sipConn.WriteToUDP([]byte(resp), peer)
			cancel()
			select {
			case <-rtpDone:
			case <-time.After(500 * time.Millisecond):
			}
			return nil
		}
	}
}

func buildSimpleSIP200(req string) string {
	lines := strings.Split(strings.ReplaceAll(req, "\r\n", "\n"), "\n")
	get := func(name string) string {
		prefix := strings.ToLower(name) + ":"
		for _, line := range lines {
			if strings.HasPrefix(strings.ToLower(line), prefix) {
				return strings.TrimSpace(line[len(prefix):])
			}
		}
		return ""
	}
	return fmt.Sprintf("SIP/2.0 200 OK\r\nVia: %s\r\nFrom: %s\r\nTo: %s\r\nCall-ID: %s\r\nCSeq: %s\r\nContent-Length: 0\r\n\r\n", get("Via"), get("From"), get("To"), get("Call-ID"), get("CSeq"))
}

func randomUint32() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint32(b[:])
}

func randomUint16() uint16 {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint16(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint16(b[:])
}

type sipResponse struct {
	statusLine string
	code       int
	headers    map[string][]string
	body       string
}

func (r sipResponse) header(name string) string {
	values := r.headers[strings.ToLower(name)]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func parseSIPResponse(raw string) (sipResponse, error) {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	partsBody := strings.SplitN(normalized, "\n\n", 2)
	lines := strings.Split(partsBody[0], "\n")
	if len(lines) == 0 {
		return sipResponse{}, errors.New("empty SIP response")
	}
	parts := strings.Fields(lines[0])
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "SIP/") {
		return sipResponse{}, errors.New("not a SIP response")
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return sipResponse{}, err
	}
	r := sipResponse{statusLine: strings.TrimSpace(lines[0]), code: code, headers: map[string][]string{}}
	if len(partsBody) == 2 {
		r.body = partsBody[1]
	}
	for _, line := range lines[1:] {
		i := strings.IndexByte(line, ':')
		if i <= 0 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[:i]))
		value := strings.TrimSpace(line[i+1:])
		r.headers[name] = append(r.headers[name], value)
	}
	return r, nil
}

type digestChallenge struct {
	realm     string
	nonce     string
	algorithm string
	qop       string
	opaque    string
}

func parseDigestChallenge(raw string) (digestChallenge, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 6 || !strings.EqualFold(raw[:6], "Digest") {
		return digestChallenge{}, fmt.Errorf("unsupported authentication scheme: %q", raw)
	}
	params := parseAuthParams(strings.TrimSpace(raw[6:]))
	c := digestChallenge{
		realm:     params["realm"],
		nonce:     params["nonce"],
		algorithm: params["algorithm"],
		opaque:    params["opaque"],
	}
	if c.realm == "" || c.nonce == "" {
		return c, errors.New("digest challenge missing realm or nonce")
	}
	if c.algorithm == "" {
		c.algorithm = "MD5"
	}
	if qops := params["qop"]; qops != "" {
		for _, q := range strings.Split(qops, ",") {
			if strings.EqualFold(strings.TrimSpace(q), "auth") {
				c.qop = "auth"
				break
			}
		}
		if c.qop == "" {
			return c, fmt.Errorf("unsupported qop %q (only auth is supported)", qops)
		}
	}
	return c, nil
}

func parseAuthParams(s string) map[string]string {
	out := map[string]string{}
	var fields []string
	start := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				fields = append(fields, s[start:i])
				start = i + 1
			}
		}
	}
	fields = append(fields, s[start:])
	for _, field := range fields {
		kv := strings.SplitN(strings.TrimSpace(field), "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(kv[0]))
		v := strings.TrimSpace(kv[1])
		if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
			v = v[1 : len(v)-1]
		}
		out[k] = v
	}
	return out
}

func buildDigestAuthorization(c digestChallenge, username, password, method, uri string) (string, error) {
	cnonce, err := randomHex(12)
	if err != nil {
		return "", err
	}
	nc := "00000001"
	hash := func(s string) (string, error) {
		switch strings.ToUpper(c.algorithm) {
		case "MD5":
			sum := md5.Sum([]byte(s))
			return hex.EncodeToString(sum[:]), nil
		case "SHA-256":
			sum := sha256.Sum256([]byte(s))
			return hex.EncodeToString(sum[:]), nil
		default:
			return "", fmt.Errorf("unsupported digest algorithm %q", c.algorithm)
		}
	}
	ha1, err := hash(username + ":" + c.realm + ":" + password)
	if err != nil {
		return "", err
	}
	ha2, err := hash(method + ":" + uri)
	if err != nil {
		return "", err
	}
	var response string
	if c.qop == "auth" {
		response, err = hash(ha1 + ":" + c.nonce + ":" + nc + ":" + cnonce + ":auth:" + ha2)
	} else {
		response, err = hash(ha1 + ":" + c.nonce + ":" + ha2)
	}
	if err != nil {
		return "", err
	}

	parts := []string{
		fmt.Sprintf(`username="%s"`, username),
		fmt.Sprintf(`realm="%s"`, c.realm),
		fmt.Sprintf(`nonce="%s"`, c.nonce),
		fmt.Sprintf(`uri="%s"`, uri),
		fmt.Sprintf(`response="%s"`, response),
		fmt.Sprintf(`algorithm=%s`, c.algorithm),
	}
	if c.opaque != "" {
		parts = append(parts, fmt.Sprintf(`opaque="%s"`, c.opaque))
	}
	if c.qop == "auth" {
		parts = append(parts, "qop=auth", "nc="+nc, fmt.Sprintf(`cnonce="%s"`, cnonce))
	}
	return "Digest " + strings.Join(parts, ", "), nil
}

func (g *gateway) sendDialoutStatus(id, roomID, callID, status, cause string, code int, message string) error {
	msg := clientMessage{
		ID:   id,
		Type: "internal",
		Internal: &internalClientMessage{
			Type: "dialout",
			Dialout: &dialoutInternalClientMessage{
				Type:   "status",
				RoomID: roomID,
				Status: &dialoutStatusMessage{
					CallID:  callID,
					Status:  status,
					Cause:   cause,
					Code:    code,
					Message: message,
				},
			},
		},
	}
	log.Printf("DIALOUT status: room=%s callid=%s status=%s", roomID, callID, status)
	return g.writeJSON(msg)
}

func (g *gateway) writeJSON(v any) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn == nil {
		return errors.New("websocket is not connected")
	}
	return g.conn.WriteJSON(v)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func newID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b)
}

func compactJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return string(raw)
	}
	return string(b)
}
