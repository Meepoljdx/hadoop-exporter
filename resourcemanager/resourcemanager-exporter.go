package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

// 设计上，resourcemanger需要手动探测活跃节点
const (
	httpsmode = false
)

var (
	listenAddress  = flag.String("web.listen-address", ":9075", "暴露指标的监听地址，默认9075.") //设置成ip:port的格式，似乎更容易进行更改
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "暴露指标的路由.")
	clientConfFile = flag.String("yarn-site.path", "/etc/hadoop/conf/yarn-site.xml", "")
	timeout        = flag.String("get.timeout-seconds", "5", "请求超时的时间")
)

//读取配置，从客户端配置中读取需要的信息
type XMLConf struct {
	XMLName   xml.Name    `xml:"configuration"`
	NameValue []NameValue `xml:"property"`
}

type NameValue struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
	Final string `xml:"final"`
}

type YARNConf struct {
	RpcPort          string //RPC端口
	ServerIP         string //ResourceManger IP
	ResourceMangerID string //ResourceManger ID
	HttpsOpen        bool   //是否开启https
	HttpPort         string //http端口
	HttpsPort        string //https端口
}

type Exporter struct {
	url string
	c   YARNConf
	// 总览信息"Hadoop:service=ResourceManager,name=ClusterMetrics"
	NumActiveNMs           prometheus.Gauge // 活动NM
	NumLostNMs             prometheus.Gauge // 失联NM
	NumDecommissioningNMs  prometheus.Gauge // 下线中的NM
	NumDecommissionedNMs   prometheus.Gauge // 已下线的NM
	NumUnhealthyNMs        prometheus.Gauge // 不健康的NM
	NumRebootedNMs         prometheus.Gauge // 重启的NM
	NumShutdownNMs         prometheus.Gauge // 已停止的NM
	AMLaunchDelayNumOps    prometheus.Gauge // AM启动数量
	AMLaunchDelayAvgTime   prometheus.Gauge // AM启动延迟
	AMRegisterDelayNumOps  prometheus.Gauge // AM注册数量
	AMRegisterDelayAvgTime prometheus.Gauge // AM注册延迟
	// 资源总览 Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default
	// 总量算法：allocated+availabled+reserved
	AllocatedVCores prometheus.Gauge // 已分配的vcore
	ReservedVCores  prometheus.Gauge // 驻留vcore
	AvailableVCores prometheus.Gauge // 空闲vcore
	PendingVCores   prometheus.Gauge // 等待分配的vcore
	AllocatedMB     prometheus.Gauge // 已分配的内存
	AvailableMB     prometheus.Gauge // 可用内存
	PendingMB       prometheus.Gauge // 等待分配的内存
	ReservedMB      prometheus.Gauge // 驻留内存
	// 任务运行指标
	AppsSubmitted prometheus.Gauge // 提交任务总数
	AppsRunning   prometheus.Gauge // 在运行的任务数
	AppsPending   prometheus.Gauge // 等待资源的任务数
	AppsCompleted prometheus.Gauge // 完成的任务数量
	AppsKilled    prometheus.Gauge // 被kill的任务数量
	AppsFailed    prometheus.Gauge // 失败任务数量
	running_0     prometheus.Gauge // 运行时间0<t<60分钟的任务
	running_60    prometheus.Gauge // 运行时间60<t<300分钟的任务
	running_300   prometheus.Gauge // 运行时间300<t<1440分钟的任务
	running_1440  prometheus.Gauge // 运行时间1440<t<∞的任务

	//RPC指标
	RpcQueueTimeNumOps       prometheus.Gauge //Rpc被调用次数 "name": "Hadoop:service=ResourceManager,name=RpcActivityForPort8030",
	RpcQueueTimeAvgTime      prometheus.Gauge //Rpc队列平均耗时
	RpcProcessingTimeNumOps  prometheus.Gauge //Rpc被调用次数，和RpcQueueTimeNumOps一样
	RpcProcessingTimeAvgTime prometheus.Gauge //Rpc平均处理耗
	//GC指标
	heapMemoryUsageCommitted prometheus.Gauge
	heapMemoryUsageInit      prometheus.Gauge //JVM内存给定值，单位为bytes
	heapMemoryUsageMax       prometheus.Gauge
	heapMemoryUsageUsed      prometheus.Gauge //JVM内存使用值，单位为bytes
	// 日志指标
	LogFatal prometheus.Gauge
	LogError prometheus.Gauge
	LogWarn  prometheus.Gauge
	LogInfo  prometheus.Gauge
	// 运行指标
	StartTime               prometheus.Gauge
	Uptime                  prometheus.Gauge //运行时长
	SystemLoadAverage       prometheus.Gauge // 操作系统平均负载 "name": "java.lang:type=OperatingSystem"
	MaxFileDescriptorCount  prometheus.Gauge
	OpenFileDescriptorCount prometheus.Gauge // 打开的文件描述符
	TotalPhysicalMemorySize prometheus.Gauge // 服务器物理内存
	FreePhysicalMemorySize  prometheus.Gauge // 空闲物理内存
	AvailableProcessors     prometheus.Gauge
	ServerActive            prometheus.Gauge // 服务状态
	//其他健康指标
	isActive prometheus.Gauge //是否是Active的
}

//用于搜索配置值，支持任意返回值类型
func SearchConf(name string, x *XMLConf) string {
	for _, v := range x.NameValue {
		//匹配配置项
		if strings.Contains(v.Name, name) {
			return v.Value
		}
	}
	return ""
}

//读取XML配置文件，返回一个XMLConf结构体
func ReadXml(path string) *XMLConf {
	xmlFile, err := os.Open(path)
	if err != nil {
		log.Error("Error opening file: %s", path)
		os.Exit(1)
	}
	defer xmlFile.Close()
	var x XMLConf
	data, err := ioutil.ReadAll(xmlFile)
	if err != nil {
		log.Error("Error reading file: %s", path)
		os.Exit(1)
	}
	err = xml.Unmarshal(data, &x)
	if err != nil {
		log.Error("Error unmarshal xml.")
		os.Exit(1)
	}
	return &x
}

//生成采集器使用的配置项
func CreateYARNConf(e *XMLConf) *YARNConf {
	c := YARNConf{}
	h, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	t, err := net.ResolveIPAddr("ip", h)
	if err != nil {
		panic(err)
	}
	c.ServerIP = t.IP.String()
	// 默认关闭https
	c.HttpsOpen = httpsmode
	for _, id := range strings.Split(SearchConf("yarn.resourcemanager.ha.rm-ids", e), ",") {
		r := "yarn.resourcemanager.resource-tracker.address." + id
		// 在yarn.resourcemanager.hostname.rm1 / rm2 中搜索是否存在主机名h，如果有则认为是这个rm
		if v := SearchConf(r, e); strings.Contains(v, h) {
			c.ResourceMangerID = id
			c.RpcPort = strings.Split(SearchConf(r, e), ":")[1]
			break
		}
	}
	// 判断是否开启HTTPS，并获取端口
	if v := SearchConf("yarn.http.policy", e); v == "HTTPS_ONLY" {
		c.HttpsOpen = true
		c.HttpsPort = strings.Split(SearchConf("yarn.resourcemanager.webapp.https.address."+c.ResourceMangerID, e), ":")[1]
	} else {
		c.HttpPort = strings.Split(SearchConf("yarn.resourcemanager.webapp.address."+c.ResourceMangerID, e), ":")[1]
	}

	return &c
}

// 指标格式定义：metrics_name{job="XX",ip="10.30.108.2",nameservice=""}

//创建指标
func NewExporter(url string, c *YARNConf) *Exporter {
	return &Exporter{
		url: url,
		c:   *c,
		NumActiveNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumActiveNms",
			Help:        "NumActiveNms",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		NumLostNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumLostNMs",
			Help:        "NumLostNMs",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		NumDecommissioningNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumDecommissioningNMs",
			Help:        "NumDecommissioningNMs",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		NumDecommissionedNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumDecommissionedNMs",
			Help:        "NumDecommissionedNMs",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		NumUnhealthyNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumUnhealthyNMs",
			Help:        "NumUnhealthyNMs",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		NumRebootedNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumRebootedNMs",
			Help:        "NumRebootedNMs",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		NumShutdownNMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_NumShutdownNMs",
			Help:        "NumShutdownNMs",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AMLaunchDelayNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AMLaunchDelayNumOps",
			Help:        "AMLaunchDelayNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AMLaunchDelayAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AMLaunchDelayAvgTime",
			Help:        "AMLaunchDelayAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AMRegisterDelayNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AMRegisterDelayNumOps",
			Help:        "AMRegisterDelayNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AMRegisterDelayAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AMRegisterDelayAvgTime",
			Help:        "AMRegisterDelayAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AllocatedVCores: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AllocatedVCores",
			Help:        "AllocatedVCores",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		ReservedVCores: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_ReservedVCores",
			Help:        "ReservedVCores",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AvailableVCores: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AvailableVCores",
			Help:        "AvailableVCores",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		PendingVCores: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_PendingVCores",
			Help:        "PendingVCores",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AllocatedMB: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AllocatedMB",
			Help:        "AllocatedMB",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AvailableMB: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AvailableMB",
			Help:        "AvailableMB",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		PendingMB: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_PendingMB",
			Help:        "PendingMB",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		ReservedMB: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_ReservedMB",
			Help:        "ReservedMB",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AppsSubmitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AppsSubmitted",
			Help:        "AppsSubmitted",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AppsRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AppsRunning",
			Help:        "AppsRunning",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AppsPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AppsPending",
			Help:        "AppsPending",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AppsCompleted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AppsCompleted",
			Help:        "AppsCompleted",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AppsKilled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AppsKilled",
			Help:        "AppsKilled",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AppsFailed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AppsFailed",
			Help:        "AppsFailed",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		running_0: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_running_0",
			Help:        "running time < 60min",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		running_60: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_running_60",
			Help:        "60min < running time < 300min",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		running_300: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_running_300",
			Help:        "300min < running time < 1440min",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		running_1440: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_running_1440",
			Help:        "running time > 1440min",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		RpcQueueTimeNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_RpcQueueTimeNumOps",
			Help:        "RpcQueueTimeNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		RpcQueueTimeAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_RpcQueueTimeAvgTime",
			Help:        "RpcQueueTimeAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		RpcProcessingTimeNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_RpcProcessingTimeNumOps",
			Help:        "RpcProcessingTimeNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		RpcProcessingTimeAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_RpcProcessingTimeAvgTime",
			Help:        "RpcProcessingTimeAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		heapMemoryUsageCommitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_heapMemoryUsageCommitted",
			Help:        "heapMemoryUsageCommitted",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		heapMemoryUsageInit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_heapMemoryUsageInit",
			Help:        "heapMemoryUsageInit",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		heapMemoryUsageMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_heapMemoryUsageMax",
			Help:        "heapMemoryUsageMax",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		heapMemoryUsageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_heapMemoryUsageUsed",
			Help:        "heapMemoryUsageUsed",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		LogFatal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_LogFatal",
			Help:        "LogFatal",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		LogError: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_LogError",
			Help:        "LogError",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		LogInfo: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_LogInfo",
			Help:        "LogInfo",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		LogWarn: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_LogWarn",
			Help:        "LogWarn",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		StartTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_StartTime",
			Help:        "StartTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		Uptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_Uptime",
			Help:        "Uptime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		SystemLoadAverage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_SystemLoadAverage",
			Help:        "SystemLoadAverage",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		OpenFileDescriptorCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_OpenFileDescriptorCount",
			Help:        "OpenFileDescriptorCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		MaxFileDescriptorCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_MaxFileDescriptorCount",
			Help:        "MaxFileDescriptorCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		TotalPhysicalMemorySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_TotalPhysicalMemorySize",
			Help:        "TotalPhysicalMemorySize",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		FreePhysicalMemorySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_FreePhysicalMemorySize",
			Help:        "FreePhysicalMemorySize",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		AvailableProcessors: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_AvailableProcessors",
			Help:        "AvailableProcessors",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		ServerActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_ServerActive",
			Help:        "ServerActive",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
		isActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "ResourceManager_isActive",
			Help:        "isActive",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "resourcemangerid": c.ResourceMangerID},
		}),
	}
}

// 定义指标的描述
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.heapMemoryUsageCommitted.Describe(ch)
	e.heapMemoryUsageInit.Describe(ch)
	e.heapMemoryUsageMax.Describe(ch)
	e.heapMemoryUsageUsed.Describe(ch)
	e.isActive.Describe(ch)
}

//采集器方法
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	// 超时处理
	t, err := strconv.Atoi(*timeout)
	client := http.Client{
		Timeout: time.Duration(t * int(time.Second)),
	}
	resp, err := client.Get(e.url)
	if err != nil {
		log.Error(err)
		e.ServerActive.Set(0)
		e.ServerActive.Collect(ch)
		return
	}
	if resp.StatusCode != 200 {
		e.ServerActive.Set(1)
		e.ServerActive.Collect(ch)
		if resp.StatusCode == 307 {
			e.isActive.Set(0)
			e.isActive.Collect(ch)
		}
		return
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil {
		log.Error(err)
	}
	m := f.(map[string]interface{})
	var nameList = m["beans"].([]interface{})
	e.ServerActive.Set(1) // 如果获取到数据了，就是活动服务
	e.isActive.Set(1)
	for _, nameData := range nameList {
		nameDataMap := nameData.(map[string]interface{})
		if nameDataMap["name"] == "Hadoop:service=ResourceManager,name=ClusterMetrics" {
			t, _ := net.ResolveIPAddr("ip", nameDataMap["tag.Hostname"].(string))
			if t.IP.String() != e.c.ServerIP {
				e.isActive.Set(0)
			}
			e.NumActiveNMs.Set(nameDataMap["NumActiveNMs"].(float64))
			e.NumLostNMs.Set(nameDataMap["NumLostNMs"].(float64))
			e.NumDecommissioningNMs.Set(nameDataMap["NumDecommissioningNMs"].(float64))
			e.NumDecommissionedNMs.Set(nameDataMap["NumDecommissionedNMs"].(float64))
			e.NumUnhealthyNMs.Set(nameDataMap["NumUnhealthyNMs"].(float64))
			e.NumRebootedNMs.Set(nameDataMap["NumRebootedNMs"].(float64))
			e.NumShutdownNMs.Set(nameDataMap["NumShutdownNMs"].(float64))
			e.AMLaunchDelayNumOps.Set(nameDataMap["AMLaunchDelayNumOps"].(float64))
			e.AMLaunchDelayAvgTime.Set(nameDataMap["AMLaunchDelayAvgTime"].(float64))
			e.AMRegisterDelayNumOps.Set(nameDataMap["AMRegisterDelayNumOps"].(float64))
			e.AMRegisterDelayAvgTime.Set(nameDataMap["AMRegisterDelayAvgTime"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default" {
			e.AllocatedVCores.Set(nameDataMap["AllocatedVCores"].(float64))
			e.ReservedVCores.Set(nameDataMap["ReservedVCores"].(float64))
			e.AvailableVCores.Set(nameDataMap["AvailableVCores"].(float64))
			e.PendingVCores.Set(nameDataMap["PendingVCores"].(float64))
			e.AllocatedMB.Set(nameDataMap["AllocatedMB"].(float64))
			e.AvailableMB.Set(nameDataMap["AvailableMB"].(float64))
			e.PendingMB.Set(nameDataMap["PendingMB"].(float64))
			e.ReservedMB.Set(nameDataMap["ReservedMB"].(float64))
			e.AppsSubmitted.Set(nameDataMap["AppsSubmitted"].(float64))
			e.AppsRunning.Set(nameDataMap["AppsRunning"].(float64))
			e.AppsPending.Set(nameDataMap["AppsPending"].(float64))
			e.AppsCompleted.Set(nameDataMap["AppsCompleted"].(float64))
			e.AppsKilled.Set(nameDataMap["AppsKilled"].(float64))
			e.AppsFailed.Set(nameDataMap["AppsFailed"].(float64))
			e.running_0.Set(nameDataMap["running_0"].(float64))
			e.running_60.Set(nameDataMap["running_60"].(float64))
			e.running_300.Set(nameDataMap["running_300"].(float64))
			e.running_1440.Set(nameDataMap["running_1440"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=ResourceManager,name=RpcActivityForPort"+e.c.RpcPort {
			e.RpcQueueTimeNumOps.Set(nameDataMap["RpcQueueTimeNumOps"].(float64))
			e.RpcQueueTimeAvgTime.Set(nameDataMap["RpcQueueTimeAvgTime"].(float64))
			e.RpcProcessingTimeNumOps.Set(nameDataMap["RpcProcessingTimeNumOps"].(float64))
			e.RpcProcessingTimeAvgTime.Set(nameDataMap["RpcProcessingTimeAvgTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Memory" {
			heapMemoryUsage := nameDataMap["HeapMemoryUsage"].(map[string]interface{})
			e.heapMemoryUsageCommitted.Set(heapMemoryUsage["committed"].(float64))
			e.heapMemoryUsageInit.Set(heapMemoryUsage["init"].(float64))
			e.heapMemoryUsageMax.Set(heapMemoryUsage["max"].(float64))
			e.heapMemoryUsageUsed.Set(heapMemoryUsage["used"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=ResourceManager,name=JvmMetrics" {
			e.LogError.Set(nameDataMap["LogError"].(float64))
			e.LogFatal.Set(nameDataMap["LogFatal"].(float64))
			e.LogInfo.Set(nameDataMap["LogInfo"].(float64))
			e.LogWarn.Set(nameDataMap["LogWarn"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Runtime" {
			e.StartTime.Set(nameDataMap["StartTime"].(float64))
			e.Uptime.Set(nameDataMap["Uptime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=OperatingSystem" {
			e.SystemLoadAverage.Set(nameDataMap["SystemLoadAverage"].(float64))
			e.OpenFileDescriptorCount.Set(nameDataMap["OpenFileDescriptorCount"].(float64))
			e.TotalPhysicalMemorySize.Set(nameDataMap["TotalPhysicalMemorySize"].(float64))
			e.FreePhysicalMemorySize.Set(nameDataMap["FreePhysicalMemorySize"].(float64))
			e.MaxFileDescriptorCount.Set(nameDataMap["MaxFileDescriptorCount"].(float64))
			e.AvailableProcessors.Set(nameDataMap["AvailableProcessors"].(float64))
		}
	}
	e.NumActiveNMs.Collect(ch)
	e.NumLostNMs.Collect(ch)
	e.NumDecommissionedNMs.Collect(ch)
	e.NumDecommissioningNMs.Collect(ch)
	e.NumUnhealthyNMs.Collect(ch)
	e.NumRebootedNMs.Collect(ch)
	e.NumShutdownNMs.Collect(ch)
	e.AMLaunchDelayNumOps.Collect(ch)
	e.AMLaunchDelayAvgTime.Collect(ch)
	e.AMRegisterDelayNumOps.Collect(ch)
	e.AMRegisterDelayAvgTime.Collect(ch)
	e.AllocatedVCores.Collect(ch)
	e.ReservedVCores.Collect(ch)
	e.AvailableVCores.Collect(ch)
	e.PendingVCores.Collect(ch)
	e.AllocatedMB.Collect(ch)
	e.AvailableMB.Collect(ch)
	e.PendingMB.Collect(ch)
	e.ReservedMB.Collect(ch)
	e.AppsSubmitted.Collect(ch)
	e.AppsRunning.Collect(ch)
	e.AppsPending.Collect(ch)
	e.AppsCompleted.Collect(ch)
	e.AppsKilled.Collect(ch)
	e.AppsFailed.Collect(ch)
	e.running_0.Collect(ch)
	e.running_60.Collect(ch)
	e.running_300.Collect(ch)
	e.running_1440.Collect(ch)
	e.RpcQueueTimeNumOps.Collect(ch)
	e.RpcQueueTimeAvgTime.Collect(ch)
	e.RpcProcessingTimeNumOps.Collect(ch)
	e.RpcProcessingTimeAvgTime.Collect(ch)
	e.heapMemoryUsageCommitted.Collect(ch)
	e.heapMemoryUsageInit.Collect(ch)
	e.heapMemoryUsageMax.Collect(ch)
	e.heapMemoryUsageUsed.Collect(ch)
	e.LogFatal.Collect(ch)
	e.LogError.Collect(ch)
	e.LogInfo.Collect(ch)
	e.LogWarn.Collect(ch)
	e.StartTime.Collect(ch)
	e.Uptime.Collect(ch)
	e.SystemLoadAverage.Collect(ch)
	e.MaxFileDescriptorCount.Collect(ch)
	e.OpenFileDescriptorCount.Collect(ch)
	e.TotalPhysicalMemorySize.Collect(ch)
	e.FreePhysicalMemorySize.Collect(ch)
	e.AvailableProcessors.Collect(ch)
	e.ServerActive.Collect(ch)
	e.isActive.Collect(ch)
}

func main() {
	flag.Parse()
	log.Info("Hadoop Exporter make By Lijiadong(Meepod) (๑•̀ㅂ•́)و✧")
	conf := CreateYARNConf(ReadXml(*clientConfFile))
	resourcemanagerJmxUrl := ""
	if conf.HttpsOpen {
		resourcemanagerJmxUrl = "https://" + conf.ServerIP + ":" + conf.HttpsPort + "/jmx"
	} else {
		resourcemanagerJmxUrl = "http://" + conf.ServerIP + ":" + conf.HttpPort + "/jmx"
	}
	exporter := NewExporter(resourcemanagerJmxUrl, conf)
	prometheus.MustRegister(exporter)
	log.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>Resourcemanager Exporter</title></head>
		<body>
		<h1>Resourcemanager Exporter By Meepo</h1>
		<h2>The greatest test of courage on earth is to bear defeat without losing heart</h2>
		<p><a href="` + *metricsPath + `">Metrics</a></p>
		</body>
		</html>`))
	})
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
