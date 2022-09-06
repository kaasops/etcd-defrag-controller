/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var (
	Endpoints string
	CAfile    string
	Certfile  string
	Keyfile   string
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
	rootCmd.Flags().StringVar(&Endpoints, "endpoints", os.Getenv("ETCD_ENDPOINTS"), "gRPC endpoints")
	rootCmd.Flags().StringVar(&CAfile, "cacert", os.Getenv("ETCD_CACERT"), "verify certificates of TLS-enabled secure servers using this CA bundle")
	rootCmd.Flags().StringVar(&Certfile, "cert", os.Getenv("ETCD_CERT"), "identify secure client using this TLS certificate file")
	rootCmd.Flags().StringVar(&Keyfile, "key", os.Getenv("ETCD_KEY"), "identify secure client using this TLS key file")
}

func StartController() error {
	c := getConnOpts()
	etcdcli, _, err := client.NewEtcdClient(c)
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), client.RequestDefaultTimeout)

	for _, endpoint := range etcdcli.Endpoints() {
		log.Printf("Start defragmenting endpoint: %s", endpoint)

		_, err := etcdcli.Defragment(ctx, endpoint)

		if err != nil {
			return err
		}
		log.Println("Finished defrag")
	}
	return nil
}

// Get connection options from cmd
func getConnOpts() client.ConnOpts {
	return client.ConnOpts{
		Endpoints:   Endpoints,
		CAfile:      CAfile,
		Certfile:    Certfile,
		Keyfile:     Keyfile,
		DialTimeout: client.DialDefaultTimeout,
	}
}
