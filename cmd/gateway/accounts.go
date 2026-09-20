package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type sipAccount struct {
	Extension     string `json:"extension"`
	Username      string `json:"username"`
	AuthUser      string `json:"auth_user,omitempty"`
	Password      string `json:"password"`
	CallerID      string `json:"caller_id,omitempty"`
	NextcloudUser string `json:"nextcloud_user"`
	Default       bool   `json:"default,omitempty"`
	Enabled       *bool  `json:"enabled,omitempty"`
}

func (a *sipAccount) isEnabled() bool {
	return a != nil && (a.Enabled == nil || *a.Enabled)
}

func (a *sipAccount) normalize() error {
	a.Extension = strings.TrimSpace(a.Extension)
	a.Username = strings.TrimSpace(a.Username)
	a.AuthUser = strings.TrimSpace(a.AuthUser)
	a.CallerID = strings.TrimSpace(a.CallerID)
	a.NextcloudUser = strings.TrimSpace(a.NextcloudUser)
	if a.Extension == "" {
		return errors.New("account extension is required")
	}
	if a.Username == "" {
		a.Username = a.Extension
	}
	if a.AuthUser == "" {
		a.AuthUser = a.Username
	}
	if a.Password == "" {
		return fmt.Errorf("account %s password is required", a.Extension)
	}
	if a.NextcloudUser == "" {
		return fmt.Errorf("account %s nextcloud_user is required", a.Extension)
	}
	return nil
}

type accountsConfig struct {
	Version  int          `json:"version"`
	Accounts []sipAccount `json:"accounts"`
}

type registrationState struct {
	account *sipAccount
	cancel  context.CancelFunc
	done    chan struct{}
}

type accountManager struct {
	cfg         config
	mu          sync.RWMutex
	byExtension map[string]*sipAccount
	byUser      map[string]*sipAccount
	accounts    []*sipAccount
	def         *sipAccount
	transport   *sipTransport

	regMu     sync.Mutex
	regs      map[string]*registrationState
	runCtx    context.Context
	apiClient *http.Client
}

func loadAccountManager(cfg config) (*accountManager, error) {
	m := &accountManager{
		cfg:         cfg,
		byExtension: make(map[string]*sipAccount),
		byUser:      make(map[string]*sipAccount),
		regs:        make(map[string]*registrationState),
		apiClient:   &http.Client{Timeout: 10 * time.Second},
	}
	ac, err := m.fetchAccounts(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load SIP accounts from Nextcloud: %w", err)
	}
	if err := m.installAccounts(ac.Accounts, false); err != nil {
		return nil, err
	}
	if len(m.accounts) == 0 {
		return nil, errors.New("Nextcloud Telephony API returned no enabled SIP accounts")
	}
	return m, nil
}

func (m *accountManager) fetchAccounts(ctx context.Context) (accountsConfig, error) {
	var ac accountsConfig
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.AccountsAPIURL, nil)
	if err != nil {
		return ac, err
	}
	req.Header.Set("Authorization", "Bearer "+m.cfg.GatewayAPIToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := m.apiClient.Do(req)
	if err != nil {
		return ac, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return ac, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(body))
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return ac, fmt.Errorf("HTTP %s: %s", resp.Status, msg)
	}
	if err := json.Unmarshal(body, &ac); err != nil {
		return ac, fmt.Errorf("decode API response: %w", err)
	}
	if ac.Version != 0 && ac.Version != 1 {
		return ac, fmt.Errorf("unsupported accounts API version %d", ac.Version)
	}
	return ac, nil
}

func normalizeAccounts(input []sipAccount) ([]*sipAccount, map[string]*sipAccount, map[string]*sipAccount, *sipAccount, error) {
	accounts := make([]*sipAccount, 0, len(input))
	byExtension := make(map[string]*sipAccount)
	byUser := make(map[string]*sipAccount)
	var def *sipAccount
	for i := range input {
		a := input[i]
		if !a.isEnabled() {
			continue
		}
		if err := a.normalize(); err != nil {
			return nil, nil, nil, nil, err
		}
		if _, exists := byExtension[a.Extension]; exists {
			return nil, nil, nil, nil, fmt.Errorf("duplicate SIP extension %s", a.Extension)
		}
		if _, exists := byUser[a.NextcloudUser]; exists {
			return nil, nil, nil, nil, fmt.Errorf("duplicate nextcloud_user %s", a.NextcloudUser)
		}
		cp := a
		p := &cp
		byExtension[p.Extension] = p
		byUser[p.NextcloudUser] = p
		accounts = append(accounts, p)
		if p.Default {
			if def != nil {
				return nil, nil, nil, nil, errors.New("only one SIP account may have default=true")
			}
			def = p
		}
	}
	if def == nil && len(accounts) == 1 {
		def = accounts[0]
	}
	return accounts, byExtension, byUser, def, nil
}

func accountEquivalent(a, b *sipAccount) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Extension == b.Extension &&
		a.Username == b.Username &&
		a.AuthUser == b.AuthUser &&
		a.Password == b.Password &&
		a.CallerID == b.CallerID &&
		a.NextcloudUser == b.NextcloudUser &&
		a.Default == b.Default
}

func (m *accountManager) installAccounts(input []sipAccount, dynamic bool) error {
	accounts, byExtension, byUser, def, err := normalizeAccounts(input)
	if err != nil {
		return err
	}

	m.mu.Lock()
	oldByExtension := m.byExtension
	// Preserve pointers for unchanged accounts so active selections and registration
	// loops continue to reference immutable objects.
	for i, a := range accounts {
		if old := oldByExtension[a.Extension]; old != nil && accountEquivalent(old, a) {
			accounts[i] = old
			byExtension[a.Extension] = old
			byUser[a.NextcloudUser] = old
			if def == a {
				def = old
			}
		}
	}
	m.accounts = accounts
	m.byExtension = byExtension
	m.byUser = byUser
	m.def = def
	m.mu.Unlock()

	if dynamic {
		m.reconcileRegistrations(oldByExtension, byExtension)
	}
	return nil
}

func (m *accountManager) reconcileRegistrations(oldMap, newMap map[string]*sipAccount) {
	for ext, old := range oldMap {
		now := newMap[ext]
		if now == nil || !accountEquivalent(old, now) {
			m.stopRegistration(ext, old, true)
		}
	}
	for ext, a := range newMap {
		old := oldMap[ext]
		if old == nil || !accountEquivalent(old, a) {
			m.startRegistration(a)
			if old == nil {
				log.Printf("SIP account added from Nextcloud: extension=%s nextcloud_user=%s", a.Extension, a.NextcloudUser)
			} else {
				log.Printf("SIP account updated from Nextcloud: extension=%s nextcloud_user=%s", a.Extension, a.NextcloudUser)
			}
		}
	}
	for ext := range oldMap {
		if newMap[ext] == nil {
			log.Printf("SIP account removed from Nextcloud: extension=%s", ext)
		}
	}
}

func (m *accountManager) SetTransport(t *sipTransport) {
	m.mu.Lock()
	m.transport = t
	m.mu.Unlock()
}

func (m *accountManager) AccountByExtension(ext string) (*sipAccount, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.byExtension[strings.TrimSpace(ext)]
	return a, ok
}

func (m *accountManager) SelectOutbound(caller, nextcloudUser string) (*sipAccount, error) {
	caller = strings.TrimSpace(caller)
	nextcloudUser = strings.TrimSpace(nextcloudUser)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if nextcloudUser != "" {
		if a := m.byUser[nextcloudUser]; a != nil {
			return a, nil
		}
	}
	if caller != "" {
		var match *sipAccount
		for _, a := range m.accounts {
			if a.CallerID != "" && a.CallerID == caller {
				if match != nil {
					match = nil
					break
				}
				match = a
			}
		}
		if match != nil {
			return match, nil
		}
	}
	if m.def != nil {
		return m.def, nil
	}
	return nil, fmt.Errorf("cannot select SIP account for nextcloud_user=%q caller=%q: no mapping and no default account", nextcloudUser, caller)
}

func (m *accountManager) LogAccounts() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, a := range m.accounts {
		marker := ""
		if m.def == a {
			marker = " default"
		}
		log.Printf("SIP account loaded from Nextcloud: extension=%s auth_user=%s nextcloud_user=%s caller_id=%s%s", a.Extension, a.AuthUser, a.NextcloudUser, a.CallerID, marker)
	}
}

func (m *accountManager) RunRegistrations(ctx context.Context) {
	m.regMu.Lock()
	m.runCtx = ctx
	m.regMu.Unlock()

	m.mu.RLock()
	current := append([]*sipAccount(nil), m.accounts...)
	m.mu.RUnlock()
	for _, a := range current {
		m.startRegistration(a)
	}

	go m.refreshLoop(ctx)
	go func() {
		<-ctx.Done()
		m.stopAllRegistrations()
	}()
}

func (m *accountManager) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.AccountsRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ac, err := m.fetchAccounts(ctx)
			if err != nil {
				log.Printf("Nextcloud SIP account refresh failed; keeping current accounts: %v", err)
				continue
			}
			if err := m.installAccounts(ac.Accounts, true); err != nil {
				log.Printf("Nextcloud SIP account refresh rejected; keeping current accounts: %v", err)
				continue
			}
		}
	}
}

func (m *accountManager) startRegistration(a *sipAccount) {
	m.regMu.Lock()
	if m.runCtx == nil || m.runCtx.Err() != nil {
		m.regMu.Unlock()
		return
	}
	if _, exists := m.regs[a.Extension]; exists {
		m.regMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(m.runCtx)
	state := &registrationState{account: a, cancel: cancel, done: make(chan struct{})}
	m.regs[a.Extension] = state
	m.regMu.Unlock()

	go func() {
		defer close(state.done)
		m.registrationLoop(ctx, a)
	}()
}

func (m *accountManager) stopRegistration(ext string, a *sipAccount, sendUnregister bool) {
	m.regMu.Lock()
	state := m.regs[ext]
	if state != nil {
		delete(m.regs, ext)
		state.cancel()
	}
	m.regMu.Unlock()
	if state != nil {
		select {
		case <-state.done:
		case <-time.After(2 * time.Second):
		}
	}
	if sendUnregister && a != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = m.unregister(ctx, a)
		cancel()
	}
}

func (m *accountManager) stopAllRegistrations() {
	m.regMu.Lock()
	states := make([]*registrationState, 0, len(m.regs))
	for ext, s := range m.regs {
		states = append(states, s)
		delete(m.regs, ext)
		s.cancel()
	}
	m.regMu.Unlock()
	for i := range states {
		select {
		case <-states[i].done:
		case <-time.After(500 * time.Millisecond):
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = m.unregister(ctx, states[i].account)
		cancel()
	}
}

func (m *accountManager) registrationLoop(ctx context.Context, a *sipAccount) {
	retry := 5 * time.Second
	for {
		expires, err := m.register(ctx, a, m.cfg.SIPRegisterExpires)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("SIP REGISTER failed: extension=%s error=%v", a.Extension, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(retry):
				continue
			}
		}
		if expires <= 0 {
			expires = m.cfg.SIPRegisterExpires
		}
		log.Printf("SIP REGISTERED: extension=%s contact=%s expires=%ds", a.Extension, m.contactURI(a), expires)
		refresh := time.Duration(float64(expires)*0.75) * time.Second
		if refresh < 30*time.Second {
			refresh = 30 * time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(refresh):
		}
	}
}

func (m *accountManager) contactURI(a *sipAccount) string {
	host, port := m.contactHostPort()
	return fmt.Sprintf("sip:%s@%s:%d", a.Extension, host, port)
}

func (m *accountManager) contactHostPort() (string, int) {
	host := strings.TrimSpace(m.cfg.SIPContactIP)
	_, pstr, _ := net.SplitHostPort(m.cfg.SIPInboundListen)
	port, _ := strconv.Atoi(pstr)
	if port == 0 {
		port = 5060
	}
	if host == "" {
		server := m.cfg.SIPServer
		if !strings.Contains(server, ":") {
			server += ":5060"
		}
		if raddr, err := net.ResolveUDPAddr("udp", server); err == nil {
			host = localIPFor(raddr)
		}
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port
}

func (m *accountManager) unregister(ctx context.Context, a *sipAccount) error {
	_, err := m.register(ctx, a, 0)
	if err == nil {
		log.Printf("SIP UNREGISTERED: extension=%s", a.Extension)
	}
	return err
}

func (m *accountManager) register(ctx context.Context, a *sipAccount, expires int) (int, error) {
	server := m.cfg.SIPServer
	if !strings.Contains(server, ":") {
		server += ":5060"
	}
	raddr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return 0, err
	}
	m.mu.RLock()
	transport := m.transport
	m.mu.RUnlock()
	if transport == nil {
		return 0, errors.New("shared SIP transport is not available")
	}
	domain := m.cfg.SIPDomain
	if domain == "" {
		domain = raddr.IP.String()
	}
	localAddr := transport.LocalAddr()
	localIP := strings.TrimSpace(m.cfg.SIPContactIP)
	if localIP == "" || localIP == "0.0.0.0" {
		localIP = localIPFor(raddr)
	}
	localPort := localAddr.Port
	uri := "sip:" + domain
	aor := fmt.Sprintf("sip:%s@%s", a.Username, domain)
	callID := newID("reg") + "@talk-gateway"
	tag := newID("rt")
	cseq := 1
	build := func(branch, auth string) string {
		var b strings.Builder
		fmt.Fprintf(&b, "REGISTER %s SIP/2.0\r\n", uri)
		fmt.Fprintf(&b, "Via: SIP/2.0/UDP %s:%d;branch=%s;rport\r\n", localIP, localPort, branch)
		fmt.Fprintf(&b, "Max-Forwards: 70\r\nFrom: <%s>;tag=%s\r\nTo: <%s>\r\n", aor, tag, aor)
		fmt.Fprintf(&b, "Call-ID: %s\r\nCSeq: %d REGISTER\r\n", callID, cseq)
		fmt.Fprintf(&b, "Contact: <%s>;expires=%d\r\nExpires: %d\r\n", m.contactURI(a), expires, expires)
		fmt.Fprintf(&b, "User-Agent: %s\r\n", userAgent)
		if auth != "" {
			fmt.Fprintf(&b, "Authorization: %s\r\n", auth)
		}
		fmt.Fprintf(&b, "Content-Length: 0\r\n\r\n")
		return b.String()
	}
	responses, unregister := transport.registerWaiter(callID)
	defer unregister()
	branch := "z9hG4bK-" + newID("reg")
	if _, err = transport.WriteToUDP([]byte(build(branch, "")), raddr); err != nil {
		return 0, err
	}
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	authTried := false
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-deadline.C:
			return 0, errors.New("REGISTER timed out")
		case env := <-responses:
			resp := env.resp
			if resp.code == 401 || resp.code == 407 {
				if authTried {
					return 0, fmt.Errorf("REGISTER authentication rejected for extension %s", a.Extension)
				}
				challengeRaw := resp.header("www-authenticate")
				if resp.code == 407 {
					challengeRaw = resp.header("proxy-authenticate")
				}
				ch, er := parseDigestChallenge(challengeRaw)
				if er != nil {
					return 0, er
				}
				auth, er := buildDigestAuthorization(ch, a.AuthUser, a.Password, "REGISTER", uri)
				if er != nil {
					return 0, er
				}
				authTried = true
				cseq++
				branch = "z9hG4bK-" + newID("reg")
				if _, er = transport.WriteToUDP([]byte(build(branch, auth)), raddr); er != nil {
					return 0, er
				}
				continue
			}
			if resp.code >= 200 && resp.code < 300 {
				got := expires
				if h := resp.header("expires"); h != "" {
					if n, e := strconv.Atoi(strings.TrimSpace(h)); e == nil {
						got = n
					}
				}
				return got, nil
			}
			if resp.code >= 300 {
				return 0, fmt.Errorf("REGISTER rejected: %s", resp.statusLine)
			}
		}
	}
}
