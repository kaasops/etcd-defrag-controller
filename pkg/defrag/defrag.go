package defrag

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"math"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
)

const (
	minDefragBytes          int64   = 500 * 1024 * 1024 // 100MB
	maxFragmentedPercentage float64 = 40
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
		if !GetMemberHealth(ctx, m, c) {
			klog.Errorf("Member %s is unhealthy. Cancel defragmentation", m.Name)
			return err
		}
		if !isMemberFragmented(m, status) {
			klog.Infof("Memeber %s is not fragmented or database less then %d bytes. Skipping", m.Name, minDefragBytes)
			continue
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
	if len(etcdMembers) != 0 {
		klog.Infof("Start defragmentation")
		for _, member := range etcdMembers {
			klog.Infof("Defragmenting endpoint: %s", member.Name)
			_, err := DefragmentMember(ctx, member, c)
			if err != nil {
				return err
			}
		}
		klog.Infof("Defragmentation finished")
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

func GetMemberHealth(ctx context.Context, member *etcdserverpb.Member, c *client.ConnOpts) bool {
	cli, err := client.NewMemberEtcdClient(member, c)
	if err != nil {
		klog.Errorf("Failed to create etcd member client %v", err)
		return false
	}
	defer func() {
		if cli == nil {
			return
		}
		if err := cli.Close(); err != nil {
			klog.Errorf("error closing etcd client for defrag: %v", err)
		}
	}()
	resp, err := cli.Get(ctx, "health")
	if err == nil && resp.Header != nil {
		return true
	}
	return false
}

func isMemberFragmented(member *etcdserverpb.Member, endpointStatus *clientv3.StatusResponse) bool {
	if endpointStatus == nil {
		klog.Errorf("endpoint status validation failed: %v", endpointStatus)
		return false
	}
	fragmentedPercentage := checkFragmentationPercentage(endpointStatus.DbSize, endpointStatus.DbSizeInUse)
	if fragmentedPercentage > 0.00 {
		klog.Infof("etcd member %q backend store fragmented: %.2f %%, dbSize: %d, dbSizeInUse: %d", member.Name, fragmentedPercentage, endpointStatus.DbSize, endpointStatus.DbSizeInUse)
	}
	return fragmentedPercentage >= maxFragmentedPercentage && endpointStatus.DbSize >= minDefragBytes

}

func checkFragmentationPercentage(ondisk, inuse int64) float64 {
	diff := float64(ondisk - inuse)
	fragmentedPercentage := (diff / float64(ondisk)) * 100
	return math.Round(fragmentedPercentage*100) / 100
}
