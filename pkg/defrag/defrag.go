package defrag

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"fmt"
	"log"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func RunDefrag(ctx context.Context, etcdcli *clientv3.Client, c *client.ConnOpts) error {
	resp, err := etcdcli.MemberList(ctx)
	if err != nil {
		return err
	}
	var etcdMembers []*etcdserverpb.Member
	var leader *etcdserverpb.Member
	for _, m := range resp.Members {
		if m.IsLearner {
			continue
		}
		if len(m.ClientURLs) == 0 {
			continue
		}
		status, err := etcdcli.Status(ctx, m.ClientURLs[0])
		if err != nil {
			return err
		}
		if leader == nil && status.Leader == m.ID {
			leader = m
			continue
		}
		etcdMembers = append(etcdMembers, m)
	}

	if leader != nil {
		etcdMembers = append(etcdMembers, leader)
	}

	for _, member := range etcdMembers {
		cli, err := client.NewMemberEtcdClient(member, c)
		if err != nil {
			return err
		}
		log.Printf("Start defragmenting endpoint: %s", member.Name)
		resp, err := cli.Get(ctx, "health")
		if err != nil {
			return err
		}
		fmt.Println(resp.Header)
		_, err = etcdcli.Defragment(ctx, member.ClientURLs[0])

		if err != nil {
			return err
		}
		log.Println("Finished defrag")
	}
	return nil
}
