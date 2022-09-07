/*
Copyright © 2022 Denis Khachyan <khachyanda@gmail.com>

*/
package cmd

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"etcd-defrag-controller/pkg/defrag"
	"os"

	"github.com/spf13/cobra"
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
	RunE: func(cmd *cobra.Command, args []string) error {
		err := StartController()
		if err != nil {
			return err
		}
		return nil
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

func StartController() error {
	c := GetConnOpts()
	etcdcli, err := client.NewEtcdClient(EndpointsCmd, c)
	if err != nil {
		return err
	}
	defer etcdcli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), client.RequestDefaultTimeout)
	defrag.RunDefrag(ctx, etcdcli, c)
	cancel()
	return nil
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
