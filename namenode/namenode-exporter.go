package main

import (
	"encoding/json"
	"encoding/xml"
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const (
	httpsmode = false
)

var (
	listenAddress = flag.String("web.listen-address", ":9070", "暴露指标的监听地址，默认9070.") //设置成ip:port的格式，似乎更容易进行更改
	metricsPath   = flag.String("web.telemetry-path", "/metrics", "暴露指标的路由.")
	//namenodeJmxUrl = flag.String("namenode.jmx.url", "http://localhost:50070/jmx", "Hadoop JMX URL.")
	clientConfFile = flag.String("hdfs-site.path", "/etc/hadoop/conf/hdfs-site.xml", "")
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

type HDFSConf struct {
	RpcPort     string //RPC端口
	ServerIP    string //NameNode IP
	NameService string //HDFS的nameservice
	NameNodeID  string //NameNode ID
	HttpsOpen   bool   //是否开启https
	HttpPort    string //http端口
	HttpsPort   string //https端口
}

type Exporter struct {
	url string
	c   HDFSConf
	//文件系统指标
	MissingBlocks         prometheus.Gauge //缺失块
	CapacityTotal         prometheus.Gauge //配置的HDFS空间
	CapacityUsed          prometheus.Gauge //使用的HDFS空间
	CapacityRemaining     prometheus.Gauge //剩余的HDFS空间
	CapacityUsedNonDFS    prometheus.Gauge //非HDFS使用的空间
	BlocksTotal           prometheus.Gauge //块总数
	FilesTotal            prometheus.Gauge //文件总数
	CorruptBlocks         prometheus.Gauge //损坏的块总数
	UnderReplicatedBlocks prometheus.Gauge //副本不足的块
	ExcessBlocks          prometheus.Gauge //多余块数量
	PendingDeletionBlocks prometheus.Gauge //等待删除的块
	NumActiveClients      prometheus.Gauge //活跃的客户端连接数
	LastCheckpointTime    prometheus.Gauge //上次检查点时间
	//DataNode健康信息
	NumLiveDataNodes            prometheus.Gauge //Namenode标记Live的DataNode数量
	NumDeadDataNodes            prometheus.Gauge //Namenode标记Dead的DataNode数量
	NumDecomLiveDataNodes       prometheus.Gauge //Namenode标记Live的下线的DataNode数量
	NumDecomDeadDataNodes       prometheus.Gauge //Namenode标记Dead的下线的DataNode数量
	NumDecommissioningDataNodes prometheus.Gauge //下线的DataNode数量
	VolumeFailuresTotal         prometheus.Gauge //坏盘数量
	StaleDataNodes              prometheus.Gauge //由于心跳延迟而标记为过期的DataNodes当前数目
	//RPC指标
	RpcQueueTimeNumOps       prometheus.Gauge //Rpc被调用次数
	RpcQueueTimeAvgTime      prometheus.Gauge //Rpc队列平均耗时
	RpcProcessingTimeNumOps  prometheus.Gauge //Rpc被调用次数，和RpcQueueTimeNumOps一样
	RpcProcessingTimeAvgTime prometheus.Gauge //Rpc平均处理耗
	//GC指标
	pnGcCount                prometheus.Gauge
	pnGcTime                 prometheus.Gauge
	cmsGcCount               prometheus.Gauge
	cmsGcTime                prometheus.Gauge
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
	Uptime                  prometheus.Gauge //运行时长
	SystemLoadAverage       prometheus.Gauge // 操作系统平均负载 "name": "java.lang:type=OperatingSystem"
	MaxFileDescriptorCount  prometheus.Gauge
	OpenFileDescriptorCount prometheus.Gauge // 打开的文件描述符
	TotalPhysicalMemorySize prometheus.Gauge // 服务器物理内存
	FreePhysicalMemorySize  prometheus.Gauge // 空闲物理内存
	AvailableProcessors     prometheus.Gauge
	ServerActive            prometheus.Gauge // 服务状态
	//其他健康指标
	isActive             prometheus.Gauge //是否是Active的
	LastHATransitionTime prometheus.Gauge //上次主备切换时间，毫秒时间戳
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
func CreateHDFSConf(e *XMLConf) *HDFSConf {
	c := HDFSConf{}
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
	c.NameService = SearchConf("dfs.internal.nameservices", e)
	for _, id := range strings.Split(SearchConf("dfs.ha.namenodes."+c.NameService, e), ",") {
		r := "dfs.namenode.rpc-address." + c.NameService + "." + id
		if v := SearchConf(r, e); strings.Contains(v, h) {
			c.NameNodeID = id
			c.RpcPort = strings.Split(SearchConf(r, e), ":")[1]
			break
		}
	}
	// 判断是否开启HTTPS，并获取端口
	if v := SearchConf("dfs.http.policy", e); v == "HTTPS_ONLY" {
		c.HttpsOpen = true
		c.HttpsPort = strings.Split(SearchConf("dfs.namenode.https-address."+c.NameService+"."+c.NameNodeID, e), ":")[1]
	} else {
		c.HttpPort = strings.Split(SearchConf("dfs.namenode.http-address."+c.NameService+"."+c.NameNodeID, e), ":")[1]
	}

	return &c
}

//指标格式定义：metrics_name{job="XX",ip="10.30.108.2",nameservice=""}

//创建指标
func NewExporter(url string, c *HDFSConf) *Exporter {
	return &Exporter{
		url: url,
		c:   *c,
		MissingBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_MissingBlocks",
			Help:        "MissingBlocks",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		CapacityTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_CapacityTotal",
			Help:        "CapacityTotal",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		CapacityUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_CapacityUsed",
			Help:        "CapacityUsed",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		CapacityRemaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_CapacityRemaining",
			Help:        "CapacityRemaining",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		CapacityUsedNonDFS: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_CapacityUsedNonDFS",
			Help:        "CapacityUsedNonDFS",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		BlocksTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_BlocksTotal",
			Help:        "BlocksTotal",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		FilesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_FilesTotal",
			Help:        "FilesTotal",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		CorruptBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_CorruptBlocks",
			Help:        "CorruptBlocks",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		UnderReplicatedBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_UnderReplicatedBlocks",
			Help:        "UnderReplicatedBlocks",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		ExcessBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_ExcessBlocks",
			Help:        "ExcessBlocks",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		PendingDeletionBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_PendingDeletionBlocks",
			Help:        "PendingDeletionBlocks",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		NumActiveClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_NumActiveClients",
			Help:        "NumActiveClients",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		LastCheckpointTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_LastCheckpointTime",
			Help:        "LastCheckpointTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		NumLiveDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_NumLiveDataNodes",
			Help:        "NameNode_NumLiveDataNodes",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		NumDeadDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_NumDeadDataNodes",
			Help:        "NumDeadDataNodes",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		NumDecomLiveDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_NumDecomLiveDataNodes",
			Help:        "NumDecomLiveDataNodes",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		NumDecomDeadDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_NumDecomDeadDataNodes",
			Help:        "NumDecomDeadDataNodes",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		NumDecommissioningDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_NumDecommissioningDataNodes",
			Help:        "NumDecommissioningDataNodes",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		VolumeFailuresTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_VolumeFailuresTotal",
			Help:        "VolumeFailuresTotal",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		StaleDataNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_StaleDataNodes",
			Help:        "StaleDataNodes",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		RpcQueueTimeNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_RpcQueueTimeNumOps",
			Help:        "RpcQueueTimeNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		RpcQueueTimeAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_RpcQueueTimeAvgTime",
			Help:        "RpcQueueTimeAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		RpcProcessingTimeNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_RpcProcessingTimeNumOps",
			Help:        "RpcProcessingTimeNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		RpcProcessingTimeAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_RpcProcessingTimeAvgTime",
			Help:        "RpcProcessingTimeAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		pnGcCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_ParNew_CollectionCount",
			Help:        "ParNew GC Count",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		pnGcTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_ParNew_CollectionTime",
			Help:        "ParNew GC Time",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		cmsGcCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_ConcurrentMarkSweep_CollectionCount",
			Help:        "ConcurrentMarkSweep GC Count",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		cmsGcTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_ConcurrentMarkSweep_CollectionTime",
			Help:        "ConcurrentMarkSweep GC Time",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		heapMemoryUsageCommitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_heapMemoryUsageCommitted",
			Help:        "heapMemoryUsageCommitted",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		heapMemoryUsageInit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_heapMemoryUsageInit",
			Help:        "heapMemoryUsageInit",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		heapMemoryUsageMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_heapMemoryUsageMax",
			Help:        "heapMemoryUsageMax",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		heapMemoryUsageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_heapMemoryUsageUsed",
			Help:        "heapMemoryUsageUsed",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		LogFatal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_LogFatal",
			Help:        "LogFatal",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		LogError: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_LogError",
			Help:        "LogError",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		LogInfo: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_LogInfo",
			Help:        "LogInfo",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		LogWarn: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_LogWarn",
			Help:        "LogWarn",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		Uptime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_Uptime",
			Help:        "Uptime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		SystemLoadAverage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_SystemLoadAverage",
			Help:        "SystemLoadAverage",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		OpenFileDescriptorCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_OpenFileDescriptorCount",
			Help:        "OpenFileDescriptorCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		MaxFileDescriptorCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_MaxFileDescriptorCount",
			Help:        "MaxFileDescriptorCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		TotalPhysicalMemorySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_TotalPhysicalMemorySize",
			Help:        "TotalPhysicalMemorySize",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		FreePhysicalMemorySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_FreePhysicalMemorySize",
			Help:        "FreePhysicalMemorySize",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		AvailableProcessors: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_AvailableProcessors",
			Help:        "AvailableProcessors",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		ServerActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_ServerActive",
			Help:        "ServerActive",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		isActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_isActive",
			Help:        "isActive",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
		LastHATransitionTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "NameNode_LastHATransitionTime",
			Help:        "LastHATransitionTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP, "nameservice": c.NameService, "namenodeid": c.NameNodeID},
		}),
	}
}

// 定义指标的描述
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.MissingBlocks.Describe(ch)
	e.CapacityTotal.Describe(ch)
	e.CapacityUsed.Describe(ch)
	e.CapacityRemaining.Describe(ch)
	e.CapacityUsedNonDFS.Describe(ch)
	e.BlocksTotal.Describe(ch)
	e.FilesTotal.Describe(ch)
	e.CorruptBlocks.Describe(ch)
	e.ExcessBlocks.Describe(ch)
	e.StaleDataNodes.Describe(ch)
	e.pnGcCount.Describe(ch)
	e.pnGcTime.Describe(ch)
	e.cmsGcCount.Describe(ch)
	e.cmsGcTime.Describe(ch)
	e.heapMemoryUsageCommitted.Describe(ch)
	e.heapMemoryUsageInit.Describe(ch)
	e.heapMemoryUsageMax.Describe(ch)
	e.heapMemoryUsageUsed.Describe(ch)
	e.isActive.Describe(ch)
}

//采集器方法
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	resp, err := http.Get(e.url)
	if err != nil {
		log.Error(err)
		e.ServerActive.Set(0)
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
	e.ServerActive.Set(1)
	for _, nameData := range nameList {
		nameDataMap := nameData.(map[string]interface{})
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystem" {
			e.MissingBlocks.Set(nameDataMap["MissingBlocks"].(float64))
			e.CapacityTotal.Set(nameDataMap["CapacityTotal"].(float64))
			e.CapacityUsed.Set(nameDataMap["CapacityUsed"].(float64))
			e.CapacityRemaining.Set(nameDataMap["CapacityRemaining"].(float64))
			e.CapacityUsedNonDFS.Set(nameDataMap["CapacityUsedNonDFS"].(float64))
			e.BlocksTotal.Set(nameDataMap["BlocksTotal"].(float64))
			e.FilesTotal.Set(nameDataMap["FilesTotal"].(float64))
			e.CorruptBlocks.Set(nameDataMap["CorruptBlocks"].(float64))
			e.UnderReplicatedBlocks.Set(nameDataMap["UnderReplicatedBlocks"].(float64))
			e.ExcessBlocks.Set(nameDataMap["ExcessBlocks"].(float64))
			e.PendingDeletionBlocks.Set(nameDataMap["PendingDeletionBlocks"].(float64))
			e.NumActiveClients.Set(nameDataMap["NumActiveClients"].(float64))
			e.LastCheckpointTime.Set(nameDataMap["LastCheckpointTime"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=FSNamesystemState" {
			e.NumLiveDataNodes.Set(nameDataMap["NumLiveDataNodes"].(float64))
			e.NumDeadDataNodes.Set(nameDataMap["NumDeadDataNodes"].(float64))
			e.NumDecomLiveDataNodes.Set(nameDataMap["NumDecomLiveDataNodes"].(float64))
			e.NumDecomDeadDataNodes.Set(nameDataMap["NumDecomDeadDataNodes"].(float64))
			e.NumDecommissioningDataNodes.Set(nameDataMap["NumDecommissioningDataNodes"].(float64))
			e.VolumeFailuresTotal.Set(nameDataMap["VolumeFailuresTotal"].(float64))
			e.StaleDataNodes.Set(nameDataMap["NumStaleDataNodes"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=RpcActivityForPort"+e.c.RpcPort {
			e.RpcQueueTimeNumOps.Set(nameDataMap["RpcQueueTimeNumOps"].(float64))
			e.RpcQueueTimeAvgTime.Set(nameDataMap["RpcQueueTimeAvgTime"].(float64))
			e.RpcProcessingTimeNumOps.Set(nameDataMap["RpcProcessingTimeNumOps"].(float64))
			e.RpcProcessingTimeAvgTime.Set(nameDataMap["RpcProcessingTimeAvgTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=GarbageCollector,name=ParNew" {
			e.pnGcCount.Set(nameDataMap["CollectionCount"].(float64))
			e.pnGcTime.Set(nameDataMap["CollectionTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=GarbageCollector,name=ConcurrentMarkSweep" {
			e.cmsGcCount.Set(nameDataMap["CollectionCount"].(float64))
			e.cmsGcTime.Set(nameDataMap["CollectionTime"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Memory" {
			heapMemoryUsage := nameDataMap["HeapMemoryUsage"].(map[string]interface{})
			e.heapMemoryUsageCommitted.Set(heapMemoryUsage["committed"].(float64))
			e.heapMemoryUsageInit.Set(heapMemoryUsage["init"].(float64))
			e.heapMemoryUsageMax.Set(heapMemoryUsage["max"].(float64))
			e.heapMemoryUsageUsed.Set(heapMemoryUsage["used"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=JvmMetrics" {
			e.LogError.Set(nameDataMap["LogError"].(float64))
			e.LogFatal.Set(nameDataMap["LogFatal"].(float64))
			e.LogInfo.Set(nameDataMap["LogInfo"].(float64))
			e.LogWarn.Set(nameDataMap["LogWarn"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Runtime" {
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
		if nameDataMap["name"] == "Hadoop:service=NameNode,name=NameNodeStatus" {
			if nameDataMap["State"] == "active" {
				e.isActive.Set(1)
			} else {
				e.isActive.Set(0)
			}
			e.LastHATransitionTime.Set(nameDataMap["LastHATransitionTime"].(float64))
		}
	}
	e.MissingBlocks.Collect(ch)
	e.CapacityTotal.Collect(ch)
	e.CapacityUsed.Collect(ch)
	e.CapacityRemaining.Collect(ch)
	e.CapacityUsedNonDFS.Collect(ch)
	e.BlocksTotal.Collect(ch)
	e.FilesTotal.Collect(ch)
	e.CorruptBlocks.Collect(ch)
	e.UnderReplicatedBlocks.Collect(ch)
	e.ExcessBlocks.Collect(ch)
	e.PendingDeletionBlocks.Collect(ch)
	e.NumActiveClients.Collect(ch)
	e.LastCheckpointTime.Collect(ch)
	e.NumLiveDataNodes.Collect(ch)
	e.NumDeadDataNodes.Collect(ch)
	e.NumDecomLiveDataNodes.Collect(ch)
	e.NumDecomDeadDataNodes.Collect(ch)
	e.NumDecommissioningDataNodes.Collect(ch)
	e.VolumeFailuresTotal.Collect(ch)
	e.StaleDataNodes.Collect(ch)
	e.RpcQueueTimeNumOps.Collect(ch)
	e.RpcQueueTimeAvgTime.Collect(ch)
	e.RpcProcessingTimeNumOps.Collect(ch)
	e.RpcProcessingTimeAvgTime.Collect(ch)
	e.pnGcCount.Collect(ch)
	e.pnGcTime.Collect(ch)
	e.cmsGcCount.Collect(ch)
	e.cmsGcTime.Collect(ch)
	e.heapMemoryUsageCommitted.Collect(ch)
	e.heapMemoryUsageInit.Collect(ch)
	e.heapMemoryUsageMax.Collect(ch)
	e.heapMemoryUsageUsed.Collect(ch)
	e.LogFatal.Collect(ch)
	e.LogError.Collect(ch)
	e.LogInfo.Collect(ch)
	e.LogWarn.Collect(ch)
	e.Uptime.Collect(ch)
	e.SystemLoadAverage.Collect(ch)
	e.MaxFileDescriptorCount.Collect(ch)
	e.OpenFileDescriptorCount.Collect(ch)
	e.TotalPhysicalMemorySize.Collect(ch)
	e.FreePhysicalMemorySize.Collect(ch)
	e.AvailableProcessors.Collect(ch)
	e.ServerActive.Collect(ch)
	e.isActive.Collect(ch)
	e.LastHATransitionTime.Collect(ch)
}

func main() {
	flag.Parse()
	log.Info("Hadoop Exporter make By Lijiadong(Meepod) (๑•̀ㅂ•́)و✧")
	conf := CreateHDFSConf(ReadXml(*clientConfFile))
	namenodeJmxUrl := ""
	if conf.HttpsOpen {
		namenodeJmxUrl = "https://" + conf.ServerIP + ":" + conf.HttpsPort + "/jmx"
	} else {
		namenodeJmxUrl = "http://" + conf.ServerIP + ":" + conf.HttpPort + "/jmx"
	}
	exporter := NewExporter(namenodeJmxUrl, conf)
	prometheus.MustRegister(exporter)
	log.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>NameNode Exporter</title></head>
		<body>
		<h1>NameNode Exporter By Meepo</h1>
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
