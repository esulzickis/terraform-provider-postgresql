package postgresql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"time"

	"github.com/lib/pq"
	"golang.org/x/net/proxy"
)

const (
	proxyDriverName = "postgresql-proxy"
	maxRetries      = 3
	retryDelay      = 2 * time.Second
)

type proxyDriver struct{}

func (d proxyDriver) Open(name string) (driver.Conn, error) {
	return pq.DialOpen(d, name)
}

func (d proxyDriver) Dial(network, address string) (net.Conn, error) {
	dialer := proxy.FromEnvironment()
	var conn net.Conn
	var err error
	for i := 0; i < maxRetries; i++ {
		conn, err = dialer.Dial(network, address)
		if err == nil {
			return conn, nil
		}
		time.Sleep(retryDelay)
	}
	return nil, err
}

func (d proxyDriver) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	dialer := proxy.FromEnvironment()
	var conn net.Conn
	var err error
	for i := 0; i < maxRetries; i++ {
		conn, err = dialer.Dial(network, address)
		if err == nil {
			return conn, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryDelay):
		}
	}
	return nil, err
}

func init() {
	sql.Register(proxyDriverName, proxyDriver{})
}
