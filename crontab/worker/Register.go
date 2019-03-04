package worker

import (
	"context"
	"fmt"
	"github.com/Hank00AAA/crontab_distributed_system/crontab/Common"
	"go.etcd.io/etcd/clientv3"
	"net"
	"time"
)

//注册节点到etcd:/cron/workers/IP地址
type Register struct{
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string //本机地址
}

var(
	G_register *Register
)

//获取本机网卡ip
func getLocalIP() (ipv4 string, err error){

	var(
		addrs []net.Addr
		addr net.Addr
		ipNet *net.IPNet //IP地址
		isIPNet bool
	)

	//获取所有网卡
	if addrs, err = net.InterfaceAddrs();err!=nil{
		return
	}

	//取第一个非localhost的网卡IP
	for _, addr = range addrs{
		//ipv4, ipv6
		//unix socket
		//判断网络地址是ip地址
		if ipNet, isIPNet = addr.(*net.IPNet);isIPNet&&!ipNet.IP.IsLoopback(){
			//跳过ipv6
			if ipNet.IP.To4()!=nil{
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = Common.ERR_NO_LOCAL_IP_FOUND

	return
}

//注册到 /cron/workers/I，并自动续租
func (register *Register)keepOnline(){

	var(
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelContext context.Context
		cancelFunc context.CancelFunc

	)

	for {
		//注册路径
		regKey = Common.JOB_WORKER_DIR + register.localIP

		cancelFunc = nil

		//创建租约
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		//自动续租
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID);err!=nil{
			goto RETRY
		}

		cancelContext, cancelFunc = context.WithCancel(context.TODO())

		//注册到etcd
		if _, err = register.kv.Put(cancelContext,regKey, "",clientv3.WithLease(leaseGrantResp.ID));err!=nil{
			goto RETRY
		}

		fmt.Println("register ok"+regKey)

		//处理续租应答
		for{
			select {
			case keepAliveResp = <- keepAliveChan:
				if keepAliveResp==nil{ //续租失败
					fmt.Println("lease fail")
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		fmt.Println("重试")
		if cancelFunc!=nil {
			cancelFunc()
		}
	}
}

func InitRegister()(err error){
	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIP string
	)

	//初始化配置
	config = clientv3.Config{
		Endpoints:G_config.EtcdEndpoints,
		DialTimeout:time.Duration(G_config.EtcdDialTimeout)*time.Millisecond,
	}

	//建立链接
	if client, err = clientv3.New(config);err!=nil{
		return
	}

	//得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	//本机IP
	if localIP, err = getLocalIP();err!=nil{
		return
	}

	//单例
	G_register = &Register{
		client:client,
		kv:kv,
		lease:lease,
		localIP:localIP,
	}

	//服务注册
	go G_register.keepOnline()


	return
}




