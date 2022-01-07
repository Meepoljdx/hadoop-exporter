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
	listenAddress  = flag.String("web.listen-address", ":9071", "暴露指标的监听地址，默认9071.") //设置成ip:port的格式，似乎更容易进行更改
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "暴露指标的路由.")
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
	RpcPort    string // RPC端口
	ServerIP   string // DataNode IP，如果本机没有DataNode实例则直接panic
	ServerPort string // DataNode Server IP
	HostName   string // DataNode 主机名
	HttpsOpen  bool   // 是否开启https
	HttpPort   string // http端口
	HttpsPort  string // https端口
}

type Exporter struct {
	url string
	c   HDFSConf
	// 文件系统指标
	VolumeFailures    prometheus.Gauge // 坏盘数量 "name": "Hadoop:service=DataNode,name=FSDatasetState",
	CapacityTotal     prometheus.Gauge // 配置总空间
	CapacityUsed      prometheus.Gauge // 使用空间
	CapacityRemaining prometheus.Gauge // 剩余空间
	XceiverCount      prometheus.Gauge // Xceiver 数量 "name": "Hadoop:service=DataNode,name=DataNodeInfo",
	// 客户端操作指标
	DatanodeNetworkErrors  prometheus.Gauge
	WritesFromRemoteClient prometheus.Gauge // 来自远程客户端写操作 QPS
	WritesFromLocalClient  prometheus.Gauge // 来自本地客户端写操作 QPS
	ReadsFromRemoteClient  prometheus.Gauge // 来自远程客户端读操作 QPS
	ReadsFromLocalClient   prometheus.Gauge // 来自本地客户端读操作 QPS
	// 读写性能指标
	ReadBlockOpAvgTime  prometheus.Gauge // Block平均读时长
	WriteBlockOpAvgTime prometheus.Gauge // Block平均写时长
	// GC指标
	heapMemoryUsageCommitted prometheus.Gauge
	heapMemoryUsageInit      prometheus.Gauge // JVM内存给定值，单位为bytes
	heapMemoryUsageMax       prometheus.Gauge // JVM内存实际可用，单位为bytes
	heapMemoryUsageUsed      prometheus.Gauge // JVM内存使用值，单位为bytes
	// RPC指标
	RpcQueueTimeNumOps       prometheus.Gauge // Rpc被调用次数
	RpcQueueTimeAvgTime      prometheus.Gauge // Rpc队列平均耗时
	RpcProcessingTimeNumOps  prometheus.Gauge // Rpc被调用次数，和RpcQueueTimeNumOps一样
	RpcProcessingTimeAvgTime prometheus.Gauge // Rpc平均处理耗
	NumOpenConnections       prometheus.Gauge // 当前连接数
	ReceivedBytes            prometheus.Gauge // 接收数据速率
	SentBytes                prometheus.Gauge // 发送数据速率
	// 其他指标
	StartTime               prometheus.Gauge // 启动时间，时间戳 "name": "java.lang:type=Runtime"
	SystemLoadAverage       prometheus.Gauge // 操作系统平均负载 "name": "java.lang:type=OperatingSystem"
	MaxFileDescriptorCount  prometheus.Gauge
	OpenFileDescriptorCount prometheus.Gauge // 打开的文件描述符
	TotalPhysicalMemorySize prometheus.Gauge // 服务器物理内存
	FreePhysicalMemorySize  prometheus.Gauge // 空闲物理内存
	AvailableProcessors     prometheus.Gauge
	ServerActive            prometheus.Gauge // 服务状态

}

//用于搜索配置值
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
	// c.HostName = h
	c.HostName = ""
	c.ServerIP = t.IP.String()
	c.RpcPort = strings.Split(SearchConf("dfs.datanode.ipc.address", e), ":")[1]
	// 默认关闭https
	c.HttpsOpen = httpsmode
	// 判断是否开启HTTPS，并获取端口
	if v := SearchConf("dfs.http.policy", e); v == "HTTPS_ONLY" {
		c.HttpsOpen = true
		c.HttpsPort = strings.Split(SearchConf("dfs.datanode.https.address", e), ":")[1]
	} else {
		c.HttpPort = strings.Split(SearchConf("dfs.datanode.http.address", e), ":")[1]
	}

	return &c
}

//指标格式定义：metrics_name{job="XX",ip="10.30.108.2"}

//创建指标
func NewExporter(url string, c *HDFSConf) *Exporter {
	return &Exporter{
		url: url,
		c:   *c,
		XceiverCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_XceiverCount",
			Help:        "XceiverCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		VolumeFailures: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_VolumeFailures",
			Help:        "VolumeFailures",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		CapacityTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_CapacityTotal",
			Help:        "CapacityTotal",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		CapacityUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_CapacityUsed",
			Help:        "CapacityUsed",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		CapacityRemaining: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_CapacityRemaining",
			Help:        "CapacityRemaining",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		DatanodeNetworkErrors: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_DatanodeNetworkErrors",
			Help:        "DatanodeNetworkErrors",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		WritesFromRemoteClient: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_WritesFromRemoteClient",
			Help:        "WritesFromRemoteClient",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		WritesFromLocalClient: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_WritesFromLocalClient",
			Help:        "WritesFromLocalClient",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		ReadsFromRemoteClient: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_ReadsFromRemoteClient",
			Help:        "ReadsFromRemoteClient",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		ReadsFromLocalClient: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_ReadsFromLocalClient",
			Help:        "ReadsFromLocalClient",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		ReadBlockOpAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_ReadBlockOpAvgTime",
			Help:        "ReadBlockOpAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		WriteBlockOpAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_WriteBlockOpAvgTime",
			Help:        "WriteBlockOpAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		heapMemoryUsageCommitted: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_heapMemoryUsageCommitted",
			Help:        "heapMemoryUsageCommitted",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		heapMemoryUsageInit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_heapMemoryUsageInit",
			Help:        "heapMemoryUsageInit",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		heapMemoryUsageMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_heapMemoryUsageMax",
			Help:        "heapMemoryUsageMax",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		heapMemoryUsageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_heapMemoryUsageUsed",
			Help:        "heapMemoryUsageUsed",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		RpcQueueTimeNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_RpcQueueTimeNumOps",
			Help:        "RpcQueueTimeNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		RpcQueueTimeAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_RpcQueueTimeAvgTime",
			Help:        "RpcQueueTimeAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		RpcProcessingTimeNumOps: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_RpcProcessingTimeNumOps",
			Help:        "RpcProcessingTimeNumOps",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		RpcProcessingTimeAvgTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_RpcProcessingTimeAvgTime",
			Help:        "RpcProcessingTimeAvgTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		NumOpenConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_NumOpenConnections",
			Help:        "NumOpenConnections",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		ReceivedBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_ReceivedBytes",
			Help:        "ReceivedBytes",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		SentBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_SentBytes",
			Help:        "SentBytes",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		StartTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_StartTime",
			Help:        "StartTime",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		SystemLoadAverage: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_SystemLoadAverage",
			Help:        "SystemLoadAverage",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		OpenFileDescriptorCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_OpenFileDescriptorCount",
			Help:        "OpenFileDescriptorCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		MaxFileDescriptorCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_MaxFileDescriptorCount",
			Help:        "MaxFileDescriptorCount",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		TotalPhysicalMemorySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_TotalPhysicalMemorySize",
			Help:        "TotalPhysicalMemorySize",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		FreePhysicalMemorySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_FreePhysicalMemorySize",
			Help:        "FreePhysicalMemorySize",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		AvailableProcessors: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_AvailableProcessors",
			Help:        "AvailableProcessors",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
		ServerActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "DataNode_ServerActive",
			Help:        "ServerActive",
			ConstLabels: map[string]string{"serverip": c.ServerIP},
		}),
	}
}

// 定义指标的描述
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.VolumeFailures.Describe(ch)

}

//采集器方法
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.ServerActive.Set(0)
	resp, err := http.Get(e.url)
	if err != nil {
		log.Error(err)
		e.ServerActive.Collect(ch)
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
	// 先设置一下Hostname，如果存在就不设置了
	if e.c.HostName == "" {
		for _, nameData := range nameList {
			nameDataMap := nameData.(map[string]interface{})
			if nameDataMap["name"] == "Hadoop:service=DataNode,name=DataNodeInfo" {
				e.c.HostName = nameDataMap["DatanodeHostname"].(string)
				e.c.ServerPort = nameDataMap["DataPort"].(string)
			}
		}
	}
	for _, nameData := range nameList {
		nameDataMap := nameData.(map[string]interface{})
		if nameDataMap["name"] == "Hadoop:service=DataNode,name=DataNodeInfo" {
			e.XceiverCount.Set(nameDataMap["XceiverCount"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=DataNode,name=FSDatasetState" {
			e.CapacityTotal.Set(nameDataMap["Capacity"].(float64))
			e.CapacityUsed.Set(nameDataMap["DfsUsed"].(float64))
			e.CapacityRemaining.Set(nameDataMap["Remaining"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=DataNode,name=DataNodeActivity-"+e.c.HostName+"-"+e.c.ServerPort {
			e.VolumeFailures.Set(nameDataMap["VolumeFailures"].(float64))
			e.ReadBlockOpAvgTime.Set(nameDataMap["ReadBlockOpAvgTime"].(float64))
			e.WriteBlockOpAvgTime.Set(nameDataMap["WriteBlockOpAvgTime"].(float64))
			e.WritesFromRemoteClient.Set(nameDataMap["WritesFromRemoteClient"].(float64))
			e.WritesFromLocalClient.Set(nameDataMap["WritesFromLocalClient"].(float64))
			e.ReadsFromRemoteClient.Set(nameDataMap["ReadsFromRemoteClient"].(float64))
			e.ReadsFromLocalClient.Set(nameDataMap["ReadsFromLocalClient"].(float64))
			e.DatanodeNetworkErrors.Set(nameDataMap["DatanodeNetworkErrors"].(float64))
		}
		if nameDataMap["name"] == "Hadoop:service=DataNode,name=RpcActivityForPort"+e.c.RpcPort {
			e.RpcQueueTimeNumOps.Set(nameDataMap["RpcQueueTimeNumOps"].(float64))
			e.RpcQueueTimeAvgTime.Set(nameDataMap["RpcQueueTimeAvgTime"].(float64))
			e.RpcProcessingTimeNumOps.Set(nameDataMap["RpcProcessingTimeNumOps"].(float64))
			e.RpcProcessingTimeAvgTime.Set(nameDataMap["RpcProcessingTimeAvgTime"].(float64))
			e.ReceivedBytes.Set(nameDataMap["ReceivedBytes"].(float64))
			e.SentBytes.Set(nameDataMap["SentBytes"].(float64))
			e.NumOpenConnections.Set(nameDataMap["NumOpenConnections"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Memory" {
			heapMemoryUsage := nameDataMap["HeapMemoryUsage"].(map[string]interface{})
			e.heapMemoryUsageCommitted.Set(heapMemoryUsage["committed"].(float64))
			e.heapMemoryUsageInit.Set(heapMemoryUsage["init"].(float64))
			e.heapMemoryUsageMax.Set(heapMemoryUsage["max"].(float64))
			e.heapMemoryUsageUsed.Set(heapMemoryUsage["used"].(float64))
		}
		if nameDataMap["name"] == "java.lang:type=Runtime" {
			e.StartTime.Set(nameDataMap["StartTime"].(float64))
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
	e.ServerActive.Set(1)
	e.VolumeFailures.Collect(ch)
	e.CapacityTotal.Collect(ch)
	e.CapacityUsed.Collect(ch)
	e.CapacityRemaining.Collect(ch)
	e.XceiverCount.Collect(ch)
	e.DatanodeNetworkErrors.Collect(ch)
	e.WritesFromLocalClient.Collect(ch)
	e.WritesFromRemoteClient.Collect(ch)
	e.ReadsFromRemoteClient.Collect(ch)
	e.ReadsFromLocalClient.Collect(ch)
	e.ReadBlockOpAvgTime.Collect(ch)
	e.WriteBlockOpAvgTime.Collect(ch)
	e.heapMemoryUsageCommitted.Collect(ch)
	e.heapMemoryUsageInit.Collect(ch)
	e.heapMemoryUsageMax.Collect(ch)
	e.heapMemoryUsageUsed.Collect(ch)
	e.RpcQueueTimeNumOps.Collect(ch)
	e.RpcQueueTimeAvgTime.Collect(ch)
	e.RpcProcessingTimeNumOps.Collect(ch)
	e.RpcProcessingTimeAvgTime.Collect(ch)
	e.NumOpenConnections.Collect(ch)
	e.ReceivedBytes.Collect(ch)
	e.SentBytes.Collect(ch)
	e.StartTime.Collect(ch)
	e.SystemLoadAverage.Collect(ch)
	e.MaxFileDescriptorCount.Collect(ch)
	e.OpenFileDescriptorCount.Collect(ch)
	e.TotalPhysicalMemorySize.Collect(ch)
	e.FreePhysicalMemorySize.Collect(ch)
	e.AvailableProcessors.Collect(ch)
	e.ServerActive.Collect(ch)
}

func main() {
	flag.Parse()
	log.Info("Hadoop Exporter make By Lijiadong(Meepod) (๑•̀ㅂ•́)و✧")
	conf := CreateHDFSConf(ReadXml(*clientConfFile))
	datanodeJmxUrl := ""
	if conf.HttpsOpen {
		datanodeJmxUrl = "https://" + conf.ServerIP + ":" + conf.HttpsPort + "/jmx"
	} else {
		datanodeJmxUrl = "http://" + conf.ServerIP + ":" + conf.HttpPort + "/jmx"
	}
	exporter := NewExporter(datanodeJmxUrl, conf)
	prometheus.MustRegister(exporter)
	log.Printf("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>DataNode Exporter</title></head>
		<body>
		<h1>DataNode Exporter By Meepo</h1>
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
