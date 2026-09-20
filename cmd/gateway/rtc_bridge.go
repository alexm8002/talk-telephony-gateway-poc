// SPDX-FileCopyrightText: 2026 2M Production Electrique
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v3"

	mediacodec "talk-telephony-gateway-poc/internal/media"
)

type rtcServerEnvelope struct {
	ID      string            `json:"id,omitempty"`
	Type    string            `json:"type"`
	Message *rtcServerMessage `json:"message,omitempty"`
	Event   *rtcServerEvent   `json:"event,omitempty"`
	Error   *serverError      `json:"error,omitempty"`
}

type rtcServerMessage struct {
	Sender *rtcEndpoint    `json:"sender,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

type rtcServerEvent struct {
	Target string           `json:"target,omitempty"`
	Type   string           `json:"type,omitempty"`
	Join   []rtcRoomSession `json:"join,omitempty"`
	Change []rtcRoomSession `json:"change,omitempty"`
	Leave  []string         `json:"leave,omitempty"`
}

type rtcRoomSession struct {
	SessionID string         `json:"sessionid"`
	UserID    string         `json:"userid,omitempty"`
	User      map[string]any `json:"user,omitempty"`
	Features  []string       `json:"features,omitempty"`
}

type rtcEndpoint struct {
	Type      string `json:"type"`
	SessionID string `json:"sessionid,omitempty"`
}

type rtcMessageData struct {
	From       string          `json:"from,omitempty"`
	To         string          `json:"to,omitempty"`
	Type       string          `json:"type"`
	SID        string          `json:"sid,omitempty"`
	RoomType   string          `json:"roomType,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	AudioCodec string          `json:"audiocodec,omitempty"`
}

type rtcSDPPayload struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type rtcCandidatePayload struct {
	Candidate webrtc.ICECandidateInit `json:"candidate"`
}

type rtcClientEnvelope struct {
	ID      string            `json:"id,omitempty"`
	Type    string            `json:"type"`
	Message *rtcClientMessage `json:"message,omitempty"`
}

type rtcClientMessage struct {
	Recipient rtcEndpoint `json:"recipient"`
	Data      any         `json:"data"`
}

type rtcBridge struct {
	session *talkCallSession

	mu        sync.Mutex
	peers     map[string]*webrtc.PeerConnection
	answers   map[string]chan rtcSDPPayload
	closed    chan struct{}
	closeOnce sync.Once

	publisher      *webrtc.PeerConnection
	publisherTrack *webrtc.TrackLocalStaticRTP
	publisherSID   string
	publisherPT    uint8

	sipConn   *net.UDPConn
	sipRemote *net.UDPAddr
	sipPT     uint8
	txSeq     uint16
	txTS      uint32
	txSSRC    uint32

	rxWebRTCPackets atomic.Uint64
	txWebRTCPackets atomic.Uint64
	rxSIPPackets    atomic.Uint64
	txSIPPackets    atomic.Uint64

	activeAudioPublisher string
	talkEnded            chan struct{}
	talkEndOnce          sync.Once
}

func (s *talkCallSession) StartMediaBridge(ctx context.Context, sipSourcePT uint8, sipConn *net.UDPConn, sipRemote *net.UDPAddr, sipTargetPT uint8) (*rtcBridge, error) {
	if sipSourcePT != 0 && sipSourcePT != 8 {
		return nil, fmt.Errorf("unsupported SIP RTP payload %d", sipSourcePT)
	}
	b := &rtcBridge{
		session:   s,
		peers:     make(map[string]*webrtc.PeerConnection),
		answers:   make(map[string]chan rtcSDPPayload),
		closed:    make(chan struct{}),
		talkEnded: make(chan struct{}),
		sipConn:   sipConn,
		sipRemote: sipRemote,
		sipPT:     sipTargetPT,
		txSeq:     randomUint16(),
		txTS:      randomUint32(),
		txSSRC:    randomUint32(),
	}

	go b.readLoop(ctx)
	for _, existing := range s.initialSessions {
		if existing.SessionID == "" || existing.SessionID == s.sessionID || existing.SessionID == s.virtualID {
			continue
		}
		if t, _ := existing.User["type"].(string); t == "phone" {
			continue
		}
		go b.requestOffer(existing.SessionID)
	}
	if err := b.startPublisher(ctx, sipSourcePT); err != nil {
		b.Close()
		return nil, err
	}
	log.Printf("WebRTC media bridge ready: SIP source PT=%d published as %s, SIP target PT=%d", sipSourcePT, codecName(sipSourcePT), sipTargetPT)
	return b, nil
}

func (b *rtcBridge) Close() {
	b.closeOnce.Do(func() {
		close(b.closed)
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, pc := range b.peers {
			_ = pc.Close()
		}
		if b.publisher != nil {
			_ = b.publisher.Close()
		}
		log.Printf("WebRTC bridge summary: sip_rx=%d webrtc_tx=%d webrtc_rx=%d sip_tx=%d",
			b.rxSIPPackets.Load(), b.txWebRTCPackets.Load(), b.rxWebRTCPackets.Load(), b.txSIPPackets.Load())
	})
}

func (b *rtcBridge) TalkEnded() <-chan struct{} {
	return b.talkEnded
}

func codecName(pt uint8) string {
	if pt == 8 {
		return "PCMA"
	}
	return "PCMU"
}

func codecCapability(pt uint8) webrtc.RTPCodecCapability {
	if pt == 8 {
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}
	}
	return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: 8000, Channels: 1}
}

func (b *rtcBridge) newPeerConnection() (*webrtc.PeerConnection, error) {
	return webrtc.NewPeerConnection(webrtc.Configuration{})
}

func (b *rtcBridge) startPublisher(ctx context.Context, sipPT uint8) error {
	pc, err := b.newPeerConnection()
	if err != nil {
		return fmt.Errorf("create publisher peer connection: %w", err)
	}
	track, err := webrtc.NewTrackLocalStaticRTP(codecCapability(sipPT), "audio", "phone")
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("create publisher track: %w", err)
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		_ = pc.Close()
		return fmt.Errorf("add publisher track: %w", err)
	}
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("WebRTC publisher ICE state: %s", state.String())
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("WebRTC publisher state: %s", state.String())
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		_ = pc.Close()
		return err
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		_ = pc.Close()
		return err
	}
	select {
	case <-gather:
	case <-ctx.Done():
		_ = pc.Close()
		return ctx.Err()
	case <-time.After(8 * time.Second):
		_ = pc.Close()
		return errors.New("WebRTC publisher ICE gathering timed out")
	}

	sid := newID("rtc-pub")
	answerCh := make(chan rtcSDPPayload, 1)
	b.mu.Lock()
	b.publisher = pc
	b.publisherTrack = track
	b.publisherSID = sid
	b.publisherPT = sipPT
	b.peers[sid] = pc
	b.answers[sid] = answerCh
	b.mu.Unlock()

	codec := "pcmu"
	if sipPT == 8 {
		codec = "pcma"
	}
	data := map[string]any{
		"to":         b.session.sessionID,
		"type":       "offer",
		"sid":        sid,
		"roomType":   "video",
		"audiocodec": codec,
		"payload": map[string]any{
			"nick": "Phone",
			"type": "offer",
			"sdp":  pc.LocalDescription().SDP,
		},
	}
	if err := b.sendMessage(b.session.sessionID, data); err != nil {
		return fmt.Errorf("send WebRTC publisher offer: %w", err)
	}
	log.Printf("WebRTC publisher offer sent: sid=%s codec=%s", sid, strings.ToUpper(codec))

	select {
	case ans := <-answerCh:
		if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: ans.SDP}); err != nil {
			return fmt.Errorf("set publisher answer: %w", err)
		}
		log.Printf("WebRTC publisher answer accepted: sid=%s", sid)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return errors.New("timed out waiting for HPB/Janus publisher answer")
	}
}

func (b *rtcBridge) PublishSIPRTP(raw []byte) error {
	if len(raw) < 12 {
		return nil
	}
	var pkt rtp.Packet
	if err := pkt.Unmarshal(raw); err != nil {
		return nil
	}
	pt := pkt.PayloadType
	if pt != 0 && pt != 8 {
		return nil
	}
	b.rxSIPPackets.Add(1)

	b.mu.Lock()
	track := b.publisherTrack
	publisherPT := b.publisherPT
	b.mu.Unlock()
	if track == nil {
		return nil
	}
	if pt != publisherPT {
		// Asterisk may send PT0 even if it selected PT8 in its SDP. Convert the
		// actual RTP payload dynamically instead of trusting the SDP blindly.
		pcm := mediacodec.DecodeG711(pkt.Payload, pt)
		pkt.Payload = mediacodec.EncodeG711(pcm, publisherPT)
		pkt.PayloadType = publisherPT
	}
	if err := track.WriteRTP(&pkt); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	b.txWebRTCPackets.Add(1)
	return nil
}

func (b *rtcBridge) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.closed:
			return
		default:
		}
		var msg rtcServerEnvelope
		if err := b.session.conn.ReadJSON(&msg); err != nil {
			select {
			case <-b.closed:
				return
			default:
				log.Printf("WebRTC HPB read ended: %v", err)
				return
			}
		}
		switch msg.Type {
		case "event":
			b.handleEvent(msg.Event)
		case "message":
			b.handleRTCMessage(msg.Message)
		case "error":
			if msg.Error != nil {
				log.Printf("WebRTC HPB error: %s %s", msg.Error.Code, msg.Error.Message)
			}
		}
	}
}

func (b *rtcBridge) handleEvent(ev *rtcServerEvent) {
	if ev == nil || ev.Target != "room" {
		return
	}
	if ev.Type == "leave" {
		b.mu.Lock()
		active := b.activeAudioPublisher
		b.mu.Unlock()
		for _, sessionID := range ev.Leave {
			if active != "" && sessionID == active {
				log.Printf("WebRTC active Talk publisher left room: session=%s", sessionID)
				b.talkEndOnce.Do(func() { close(b.talkEnded) })
				return
			}
		}
		return
	}
	if ev.Type != "join" && ev.Type != "change" {
		return
	}
	sessions := ev.Join
	if ev.Type == "change" {
		sessions = ev.Change
	}
	for _, s := range sessions {
		if s.SessionID == "" || s.SessionID == b.session.sessionID || s.SessionID == b.session.virtualID {
			continue
		}
		if t, _ := s.User["type"].(string); t == "phone" {
			continue
		}
		go b.requestOffer(s.SessionID)
	}
}

func (b *rtcBridge) requestOffer(sessionID string) {
	data := map[string]any{"type": "requestoffer", "roomType": "video"}
	if err := b.sendMessage(sessionID, data); err != nil {
		log.Printf("WebRTC requestoffer failed for %s: %v", sessionID, err)
		return
	}
	log.Printf("WebRTC requested audio offer from Talk session=%s", sessionID)
}

func (b *rtcBridge) handleRTCMessage(m *rtcServerMessage) {
	if m == nil || len(m.Data) == 0 {
		return
	}
	var data rtcMessageData
	if err := json.Unmarshal(m.Data, &data); err != nil {
		return
	}
	switch data.Type {
	case "answer":
		var p rtcSDPPayload
		if json.Unmarshal(data.Payload, &p) != nil || p.SDP == "" {
			return
		}
		b.mu.Lock()
		ch := b.answers[data.SID]
		b.mu.Unlock()
		if ch != nil {
			select {
			case ch <- p:
			default:
			}
		}
	case "offer":
		if m.Sender == nil || m.Sender.SessionID == "" {
			return
		}
		var p rtcSDPPayload
		if json.Unmarshal(data.Payload, &p) != nil || p.SDP == "" {
			return
		}
		go b.acceptSubscriberOffer(m.Sender.SessionID, data.SID, p.SDP)
	case "candidate":
		var p rtcCandidatePayload
		if json.Unmarshal(data.Payload, &p) != nil {
			return
		}
		b.mu.Lock()
		pc := b.peers[data.SID]
		b.mu.Unlock()
		if pc != nil {
			_ = pc.AddICECandidate(p.Candidate)
		}
	}
}

func (b *rtcBridge) acceptSubscriberOffer(publisherSession, sid, offerSDP string) {
	b.mu.Lock()
	if _, exists := b.peers[sid]; exists {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	pc, err := b.newPeerConnection()
	if err != nil {
		log.Printf("WebRTC subscriber create failed: %v", err)
		return
	}
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("WebRTC subscriber ICE state: publisher=%s state=%s", publisherSession, state.String())
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		mime := strings.ToLower(track.Codec().MimeType)
		if !strings.HasPrefix(mime, "audio/") {
			log.Printf("WebRTC non-audio track ignored: publisher=%s codec=%s pt=%d", publisherSession, track.Codec().MimeType, track.PayloadType())
			return
		}
		b.mu.Lock()
		if b.activeAudioPublisher == "" {
			b.activeAudioPublisher = publisherSession
			log.Printf("WebRTC active Talk audio publisher selected: session=%s", publisherSession)
		}
		active := b.activeAudioPublisher
		b.mu.Unlock()
		if active != publisherSession {
			log.Printf("WebRTC additional Talk audio publisher ignored: session=%s active=%s", publisherSession, active)
			return
		}
		log.Printf("WebRTC Talk audio received: publisher=%s codec=%s pt=%d", publisherSession, track.Codec().MimeType, track.PayloadType())
		b.forwardTalkTrack(track)
	})
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
		_ = pc.Close()
		log.Printf("WebRTC subscriber remote offer rejected: %v", err)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		return
	}
	select {
	case <-gather:
	case <-time.After(8 * time.Second):
		_ = pc.Close()
		log.Printf("WebRTC subscriber ICE gathering timeout: publisher=%s", publisherSession)
		return
	case <-b.closed:
		_ = pc.Close()
		return
	}

	b.mu.Lock()
	b.peers[sid] = pc
	b.mu.Unlock()
	data := map[string]any{
		"to":       publisherSession,
		"type":     "answer",
		"sid":      sid,
		"roomType": "video",
		"payload": map[string]any{
			"nick": "Phone",
			"type": "answer",
			"sdp":  pc.LocalDescription().SDP,
		},
	}
	if err := b.sendMessage(publisherSession, data); err != nil {
		log.Printf("WebRTC subscriber answer send failed: %v", err)
		return
	}
	log.Printf("WebRTC subscriber answer sent: publisher=%s sid=%s", publisherSession, sid)
}

func (b *rtcBridge) forwardTalkTrack(track *webrtc.TrackRemote) {
	mime := strings.ToLower(track.Codec().MimeType)
	var opusDec *mediacodec.OpusDecoder
	if mime == strings.ToLower(webrtc.MimeTypeOpus) {
		var err error
		opusDec, err = mediacodec.NewOpusDecoder()
		if err != nil {
			log.Printf("Opus decoder unavailable: %v", err)
			return
		}
		defer opusDec.Close()
	}
	buf := make([]byte, 4096)
	for {
		n, _, err := track.Read(buf)
		if err != nil {
			return
		}
		var pkt rtp.Packet
		if pkt.Unmarshal(buf[:n]) != nil {
			continue
		}
		b.rxWebRTCPackets.Add(1)

		var pcm8 []int16
		switch mime {
		case strings.ToLower(webrtc.MimeTypePCMU):
			pcm8 = mediacodec.DecodeG711(pkt.Payload, 0)
		case strings.ToLower(webrtc.MimeTypePCMA):
			pcm8 = mediacodec.DecodeG711(pkt.Payload, 8)
		case strings.ToLower(webrtc.MimeTypeOpus):
			pcm48, err := opusDec.Decode(pkt.Payload)
			if err != nil {
				log.Printf("Opus decode failed: %v", err)
				continue
			}
			pcm8 = mediacodec.Downsample48To8(pcm48)
		default:
			continue
		}
		payload := mediacodec.EncodeG711(pcm8, b.sipPT)
		if len(payload) == 0 {
			continue
		}
		out := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: b.sipPT, SequenceNumber: b.txSeq, Timestamp: b.txTS, SSRC: b.txSSRC}, Payload: payload}
		raw, err := out.Marshal()
		if err != nil {
			continue
		}
		if _, err := b.sipConn.WriteToUDP(raw, b.sipRemote); err != nil {
			return
		}
		b.txSIPPackets.Add(1)
		b.txSeq++
		b.txTS += uint32(len(pcm8))
	}
}

func (b *rtcBridge) sendMessage(sessionID string, data any) error {
	return b.session.conn.WriteJSON(rtcClientEnvelope{
		ID:   newID("rtc"),
		Type: "message",
		Message: &rtcClientMessage{
			Recipient: rtcEndpoint{Type: "session", SessionID: sessionID},
			Data:      data,
		},
	})
}

func firstRTPPayloadType(raw []byte) (uint8, error) {
	if len(raw) < 12 {
		return 0, errors.New("RTP packet too short")
	}
	return raw[1] & 0x7f, nil
}

func buildRTPPacket(pt uint8, seq uint16, ts, ssrc uint32, payload []byte) []byte {
	out := make([]byte, 12+len(payload))
	out[0] = 0x80
	out[1] = pt
	binary.BigEndian.PutUint16(out[2:4], seq)
	binary.BigEndian.PutUint32(out[4:8], ts)
	binary.BigEndian.PutUint32(out[8:12], ssrc)
	copy(out[12:], payload)
	return out
}
