/*
Copyright © 2022 Denis Khachyan <khachyanda@gmail.com>
*/
package cmd

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"etcd-defrag-controller/pkg/defrag"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var (
	EndpointsCmd                 string
	CAfileCmd                    string
	CertfileCmd                  string
	KeyfileCmd                   string
	MaxFragmentedPercentageCmd   int
	FragmentationCheckTimeoutCmd int
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
	rootCmd.Flags().IntVar(&MaxFragmentedPercentageCmd, "maxfragmented", 40, "Maximum fragmented to start defragmentation")
	rootCmd.Flags().IntVar(&FragmentationCheckTimeoutCmd, "checkinterval", 10, "Fragmentation check interval, hours")
}

func StartController() {
	for {
		c := GetConnOpts()
		d := GetDefragOpts()
		if c.Endpoints == "" {
			log.Fatal("Missing endpoints")
		}
		ctx, cancel := context.WithTimeout(context.Background(), client.RequestDefaultTimeout)
		dc, err := defrag.NewDefragController(ctx, c, d)
		if err != nil {
			log.Fatal("Failed to start defrag controller")
		}
		klog.Info("Controller started")
		err = dc.RunDefrag()
		if err != nil {
			klog.Errorf("Defragment error: %v", err)
		}
		dc.Client.Close()
		cancel()
		time.Sleep(d.FragmentationCheckTimeout)
	}
}

// Get connection options from cmd
func GetConnOpts() *client.ConnOpts {
	return &client.ConnOpts{
		Endpoints:   EndpointsCmd,
		CAfile:      CAfileCmd,
		Certfile:    CertfileCmd,
		Keyfile:     KeyfileCmd,
		DialTimeout: client.DialDefaultTimeout,
	}
}

func GetDefragOpts() *defrag.DefragOpts {
	return &defrag.DefragOpts{
		MaxFragmentedPercentage:   MaxFragmentedPercentageCmd,
		FragmentationCheckTimeout: time.Duration(FragmentationCheckTimeoutCmd) * time.Hour,
	}
}
