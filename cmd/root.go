/*
Copyright © 2022 Denis Khachyan <khachyanda@gmail.com>

*/
package cmd

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"etcd-defrag-controller/pkg/defrag"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

const (
	DefragCheckTimeout = 12 * time.Hour
)

var (
	EndpointsCmd string
	CAfileCmd    string
	CertfileCmd  string
	KeyfileCmd   string
)

var rootCmd = &cobra.Command{
	Use:   "etcd-defrag-controller",
	Short: "Controller to defragment kubernetes etcd database",
	Long:  `Controller to defragment kubernetes etcd database`,
	Run: func(cmd *cobra.Command, args []string) {
		StartController()
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&EndpointsCmd, "endpoints", os.Getenv("ETCD_ENDPOINTS"), "gRPC endpoints")
	rootCmd.Flags().StringVar(&CAfileCmd, "cacert", os.Getenv("ETCD_CACERT"), "verify certificates of TLS-enabled secure servers using this CA bundle")
	rootCmd.Flags().StringVar(&CertfileCmd, "cert", os.Getenv("ETCD_CERT"), "identify secure client using this TLS certificate file")
	rootCmd.Flags().StringVar(&KeyfileCmd, "key", os.Getenv("ETCD_KEY"), "identify secure client using this TLS key file")

}

func StartController() {
	for {
		c := GetConnOpts()
		etcdcli, err := client.NewEtcdClient(EndpointsCmd, c)
		if err != nil {
			klog.Fatal("Error creating new etcd client %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), client.RequestDefaultTimeout)
		err = defrag.RunDefrag(ctx, etcdcli, c)
		if err != nil {
			klog.Errorf("Defragment error: %v", err)
		}
		etcdcli.Close()
		cancel()
		time.Sleep(DefragCheckTimeout)
	}
}

// Get connection options from cmd
func GetConnOpts() *client.ConnOpts {
	return &client.ConnOpts{
		CAfile:      CAfileCmd,
		Certfile:    CertfileCmd,
		Keyfile:     KeyfileCmd,
		DialTimeout: client.DialDefaultTimeout,
	}
}
