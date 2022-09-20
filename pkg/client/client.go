package client

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ConnOpts struct {
	Endpoints   string
	CAfile      string
	Certfile    string
	Keyfile     string
	DialTimeout time.Duration
}

func NewEtcdClient(c *ConnOpts) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		DialTimeout: c.DialTimeout,
		Endpoints:   endpoinsToList(c.Endpoints),
	}
	if c.CAfile != "" && c.Certfile != "" && c.Keyfile != "" {
		tlsConfig, err := NewTLSConfig(c)
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsConfig
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func NewMemberEtcdClient(member *etcdserverpb.Member, c *ConnOpts) (*clientv3.Client, error) {
	if len(member.ClientURLs) == 0 {
		log.Fatalf("Member %s is not ready", member.Name)
		return nil, nil
	}
	c.Endpoints = member.ClientURLs[0]
	cli, err := NewEtcdClient(c)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func NewTLSConfig(c *ConnOpts) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.Certfile, c.Keyfile)
	if err != nil {
		return nil, err
	}

	cacert, err := ioutil.ReadFile(c.CAfile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(cacert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}

	return tlsConfig, nil
}

func endpoinsToList(endpoints string) []string {
	return strings.Split(endpoints, ",")
}
