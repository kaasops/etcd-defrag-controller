package defrag

import (
	"context"
	"etcd-defrag-controller/pkg/client"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
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
		klog.Infof("Start defragmenting endpoint: %s", member.Name)
		_, err := DefragmentMember(ctx, member, c)
		if err != nil {
			return err
		}
		klog.Infof("Finished defrag")
	}
	return nil
}

func DefragmentMember(ctx context.Context, member *etcdserverpb.Member, c *client.ConnOpts) (*clientv3.DefragmentResponse, error) {

	cli, err := client.NewMemberEtcdClient(member, c)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cli == nil {
			return
		}
		if err := cli.Close(); err != nil {
			klog.Errorf("error closing etcd client for defrag: %v", err)
		}
	}()
	resp, err := cli.Defragment(ctx, member.ClientURLs[0])

	if err != nil {
		return nil, err
	}
	return resp, nil
}
