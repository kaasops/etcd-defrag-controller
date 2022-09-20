package defrag

import (
	"context"
	"etcd-defrag-controller/pkg/client"
	"math"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"
)

const (
	minDefragBytes int64 = 500 * 1024 * 1024 // 100MB
)

type DefragOpts struct {
	MaxFragmentedPercentage   int
	FragmentationCheckTimeout time.Duration
}

type DefragController struct {
	Ctx        context.Context
	Client     *clientv3.Client
	ClientOpts *client.ConnOpts
	DefragOpts *DefragOpts
}

func NewDefragController(ctx context.Context, copts *client.ConnOpts, dopts *DefragOpts) (*DefragController, error) {
	etcdcli, err := client.NewEtcdClient(copts)
	if err != nil {
		klog.Fatal("Error creating new etcd client %v", err)
		return nil, err
	}
	return &DefragController{
		Ctx:        ctx,
		Client:     etcdcli,
		ClientOpts: copts,
		DefragOpts: dopts,
	}, nil
}

func (d *DefragController) RunDefrag() error {
	resp, err := d.Client.MemberList(d.Ctx)
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
		status, err := d.Client.Status(d.Ctx, m.ClientURLs[0])
		if err != nil {
			return err
		}
		if !d.GetMemberHealth(m, d.ClientOpts) {
			klog.Errorf("Member %s is unhealthy. Cancel defragmentation", m.Name)
			return err
		}
		if !d.isMemberFragmented(m, status) {
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
			_, err := d.DefragmentMember(member)
			if err != nil {
				return err
			}
		}
		klog.Infof("Defragmentation finished")
	}
	return nil
}

func (d *DefragController) DefragmentMember(member *etcdserverpb.Member) (*clientv3.DefragmentResponse, error) {

	cli, err := client.NewMemberEtcdClient(member, d.ClientOpts)
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
	resp, err := cli.Defragment(d.Ctx, member.ClientURLs[0])

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (d *DefragController) GetMemberHealth(member *etcdserverpb.Member, c *client.ConnOpts) bool {
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
	resp, err := cli.Get(d.Ctx, "health")
	if err == nil && resp.Header != nil {
		return true
	}
	return false
}

func (d *DefragController) isMemberFragmented(member *etcdserverpb.Member, endpointStatus *clientv3.StatusResponse) bool {
	if endpointStatus == nil {
		klog.Errorf("endpoint status validation failed: %v", endpointStatus)
		return false
	}
	fragmentedPercentage := checkFragmentationPercentage(endpointStatus.DbSize, endpointStatus.DbSizeInUse)
	if fragmentedPercentage > 0.00 {
		klog.Infof("etcd member %q backend store fragmented: %.2f %%, dbSize: %d, dbSizeInUse: %d", member.Name, fragmentedPercentage, endpointStatus.DbSize, endpointStatus.DbSizeInUse)
	}
	return fragmentedPercentage >= float64(d.DefragOpts.MaxFragmentedPercentage) && endpointStatus.DbSize >= minDefragBytes

}

func checkFragmentationPercentage(ondisk, inuse int64) float64 {
	diff := float64(ondisk - inuse)
	fragmentedPercentage := (diff / float64(ondisk)) * 100
	return math.Round(fragmentedPercentage*100) / 100
}
