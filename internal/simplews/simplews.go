package simplews

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1" // RFC 6455 mandates SHA-1 for Sec-WebSocket-Accept.
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Conn struct {
	conn net.Conn
	br   *bufio.Reader
	mu   sync.Mutex
}

func Dial(ctx context.Context, rawURL, userAgent string) (*Conn, *http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, nil, fmt.Errorf("unsupported websocket scheme %q", u.Scheme)
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if u.Scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	}

	d := net.Dialer{}
	rawConn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, nil, err
	}
	conn := rawConn
	if u.Scheme == "wss" {
		tlsConn := tls.Client(rawConn, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			rawConn.Close()
			return nil, nil, err
		}
		conn = tlsConn
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}

	hostHeader := u.Host
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&b, "Host: %s\r\n", hostHeader)
	fmt.Fprintf(&b, "Upgrade: websocket\r\n")
	fmt.Fprintf(&b, "Connection: Upgrade\r\n")
	fmt.Fprintf(&b, "Sec-WebSocket-Key: %s\r\n", key)
	fmt.Fprintf(&b, "Sec-WebSocket-Version: 13\r\n")
	if userAgent != "" {
		fmt.Fprintf(&b, "User-Agent: %s\r\n", userAgent)
	}
	fmt.Fprintf(&b, "\r\n")

	if _, err := io.WriteString(conn, b.String()); err != nil {
		conn.Close()
		return nil, nil, err
	}

	br := bufio.NewReader(conn)
	req := &http.Request{Method: http.MethodGet, URL: u}
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, resp, fmt.Errorf("websocket upgrade failed: %s", resp.Status)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		conn.Close()
		return nil, resp, errors.New("invalid Upgrade response header")
	}
	acceptInput := []byte(key + websocketGUID)
	h := sha1.Sum(acceptInput)
	expectedAccept := base64.StdEncoding.EncodeToString(h[:])
	if resp.Header.Get("Sec-WebSocket-Accept") != expectedAccept {
		conn.Close()
		return nil, resp, errors.New("invalid Sec-WebSocket-Accept")
	}

	return &Conn{conn: conn, br: br}, resp, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.writeFrameLocked(0x8, []byte{})
	return c.conn.Close()
}

func (c *Conn) WritePing(payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeFrameLocked(0x9, payload)
}

func (c *Conn) WriteJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeFrameLocked(0x1, data)
}

func (c *Conn) ReadJSON(v any) error {
	for {
		opcode, payload, err := c.readMessage()
		if err != nil {
			return err
		}
		switch opcode {
		case 0x1:
			return json.Unmarshal(payload, v)
		case 0x8:
			return io.EOF
		case 0x9:
			c.mu.Lock()
			err := c.writeFrameLocked(0xA, payload)
			c.mu.Unlock()
			if err != nil {
				return err
			}
		case 0xA:
			// Pong, nothing to do.
		}
	}
}

func (c *Conn) readMessage() (byte, []byte, error) {
	var message []byte
	var messageOpcode byte
	for {
		header := make([]byte, 2)
		if _, err := io.ReadFull(c.br, header); err != nil {
			return 0, nil, err
		}
		fin := header[0]&0x80 != 0
		opcode := header[0] & 0x0f
		masked := header[1]&0x80 != 0
		length := uint64(header[1] & 0x7f)

		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return 0, nil, err
			}
			length = uint64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return 0, nil, err
			}
			length = binary.BigEndian.Uint64(ext[:])
		}
		if length > 16*1024*1024 {
			return 0, nil, fmt.Errorf("websocket frame too large: %d", length)
		}

		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.br, mask[:]); err != nil {
				return 0, nil, err
			}
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= mask[i%4]
			}
		}

		if opcode == 0x8 || opcode == 0x9 || opcode == 0xA {
			return opcode, payload, nil
		}
		if opcode != 0 {
			messageOpcode = opcode
		}
		message = append(message, payload...)
		if fin {
			return messageOpcode, message, nil
		}
	}
}

func (c *Conn) writeFrameLocked(opcode byte, payload []byte) error {
	if c.conn == nil {
		return errors.New("websocket connection is nil")
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})

	var header []byte
	header = append(header, 0x80|(opcode&0x0f))
	length := len(payload)
	switch {
	case length < 126:
		header = append(header, 0x80|byte(length))
	case length <= 65535:
		header = append(header, 0x80|126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(length))
	default:
		header = append(header, 0x80|127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(length))
		header = append(header, ext...)
	}

	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	header = append(header, mask[:]...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(masked)
	return err
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}
