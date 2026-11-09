package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"k8s.io/client-go/rest"
)

const (
	serialConsoleTimeout = 30 * time.Second
	plainStreamProtocol  = "plain.kubevirt.io"
)

// openSerialConsole uses KubeVirt's websocket console subresource directly so
// the PoC does not need the much broader kubevirt.io/client-go module.
func openSerialConsole(ctx context.Context, config *rest.Config, namespace, name string) (net.Conn, error) {
	connectCtx, cancel := context.WithTimeout(ctx, serialConsoleTimeout)
	defer cancel()

	var lastErr error
	for {
		conn, statusCode, err := openSerialConsoleOnce(connectCtx, config, namespace, name)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if statusCode != http.StatusBadRequest {
			return nil, lastErr
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-connectCtx.Done():
			timer.Stop()
			return nil, fmt.Errorf("timed out waiting for serial console: %w", lastErr)
		case <-timer.C:
		}
	}
}

func openSerialConsoleOnce(ctx context.Context, config *rest.Config, namespace, name string) (net.Conn, int, error) {
	requestURL, err := serialConsoleURL(config, namespace, name)
	if err != nil {
		return nil, 0, err
	}

	tlsConfig, err := rest.TLSConfigFor(config)
	if err != nil {
		return nil, 0, fmt.Errorf("configure serial console TLS: %w", err)
	}
	proxy := http.ProxyFromEnvironment
	if config.Proxy != nil {
		proxy = config.Proxy
	}
	dialer := &websocket.Dialer{
		Proxy:            proxy,
		TLSClientConfig:  tlsConfig,
		HandshakeTimeout: serialConsoleTimeout,
		Subprotocols:     []string{plainStreamProtocol},
	}

	connected := make(chan *websocket.Conn, 1)
	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) }
	transport := &consoleWebsocketRoundTripper{
		dialer:    dialer,
		connected: connected,
		done:      done,
	}
	wrappedTransport, err := rest.HTTPWrappersForConfig(config, transport)
	if err != nil {
		return nil, 0, fmt.Errorf("configure serial console authentication: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create serial console request: %w", err)
	}

	type roundTripResult struct {
		statusCode int
		err        error
	}
	result := make(chan roundTripResult, 1)
	go func() {
		response, err := wrappedTransport.RoundTrip(request)
		statusCode := 0
		if response != nil {
			statusCode = response.StatusCode
		}
		result <- roundTripResult{statusCode: statusCode, err: err}
	}()

	select {
	case conn := <-connected:
		return &serialConsoleConn{conn: conn, done: closeDone}, http.StatusSwitchingProtocols, nil
	case trip := <-result:
		closeDone()
		if trip.err == nil {
			trip.err = fmt.Errorf("websocket handshake completed without a connection")
		}
		return nil, trip.statusCode, trip.err
	case <-ctx.Done():
		closeDone()
		select {
		case conn := <-connected:
			_ = conn.Close()
		default:
		}
		return nil, 0, ctx.Err()
	}
}

func serialConsoleURL(config *rest.Config, namespace, name string) (*url.URL, error) {
	u, err := url.Parse(config.Host)
	if err != nil {
		return nil, fmt.Errorf("parse Kubernetes API URL: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return nil, fmt.Errorf("unsupported Kubernetes API URL scheme %q", u.Scheme)
	}
	u.Path = path.Join(
		u.Path,
		"/apis/subresources.kubevirt.io/v1/namespaces/"+namespace+"/virtualmachineinstances/"+name+"/console",
	)
	return u, nil
}

type consoleWebsocketRoundTripper struct {
	dialer    *websocket.Dialer
	connected chan<- *websocket.Conn
	done      <-chan struct{}
}

func (t *consoleWebsocketRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	conn, response, err := t.dialer.DialContext(request.Context(), request.URL.String(), request.Header)
	if err != nil {
		return response, err
	}
	select {
	case t.connected <- conn:
	case <-request.Context().Done():
		_ = conn.Close()
		return response, request.Context().Err()
	}
	if err := request.Context().Err(); err != nil {
		_ = conn.Close()
		return response, err
	}
	<-t.done
	return response, nil
}

type serialConsoleConn struct {
	conn      *websocket.Conn
	done      func()
	reader    io.Reader
	closeOnce sync.Once
	closeErr  error
}

func (c *serialConsoleConn) Read(p []byte) (int, error) {
	for {
		if c.reader == nil {
			messageType, reader, err := c.conn.NextReader()
			if err != nil {
				return 0, websocketStreamError(err)
			}
			if messageType != websocket.BinaryMessage {
				continue
			}
			c.reader = reader
		}

		n, err := c.reader.Read(p)
		if err == io.EOF {
			c.reader = nil
			if n == 0 {
				continue
			}
			return n, nil
		}
		return n, err
	}
}

func (c *serialConsoleConn) Write(p []byte) (int, error) {
	writer, err := c.conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return 0, websocketStreamError(err)
	}
	n, writeErr := writer.Write(p)
	closeErr := writer.Close()
	if writeErr != nil {
		return n, writeErr
	}
	return n, closeErr
}

func (c *serialConsoleConn) Close() error {
	c.closeOnce.Do(func() {
		c.done()
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

func (c *serialConsoleConn) LocalAddr() net.Addr  { return c.conn.UnderlyingConn().LocalAddr() }
func (c *serialConsoleConn) RemoteAddr() net.Addr { return c.conn.UnderlyingConn().RemoteAddr() }
func (c *serialConsoleConn) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(t)
}
func (c *serialConsoleConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *serialConsoleConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

func websocketStreamError(err error) error {
	if closeErr, ok := err.(*websocket.CloseError); ok && closeErr.Code == websocket.CloseNormalClosure {
		return io.EOF
	}
	return err
}
