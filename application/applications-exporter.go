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

const (
	httpsmode = false
	// yarn.resourcemanager.webapp.cross-origin.enabled = true 必须开启，否则任务指标无法采集
)

var (
	listenAddress  = flag.String("web.listen-address", ":9077", "暴露指标的监听地址，默认9077.") //设置成ip:port的格式，似乎更容易进行更改
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "暴露指标的路由.")
	clientConfFile = flag.String("yarn-site.path", "/etc/hadoop/conf/yarn-site.xml", "YARN的客户端配置路径，支持绝对路径和相对路径")
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
	activeServerIP      string //ResourceManger IP
	activeRMID          string //ResourceManger ID
	ResourmanagerIPList []string
	HttpsOpen           bool   //是否开启https
	HttpPort            string //http端口
	HttpsPort           string //https端口
}

type Exporter struct {
	url string
	c   YARNConf
	// 任务监控指标
	applicationState *prometheus.Desc
	startedTime      *prometheus.Desc // 任务开始时间
	finishedTime     *prometheus.Desc // 任务结束时间
	elapsedTime      *prometheus.Desc // 任务持续时间
	memorySeconds    *prometheus.Desc // 内存占用时间 mem * elapsedtime
	vcoreSeconds     *prometheus.Desc // CPU占用时间 cpu * elapsedtime
	// 以下指标仅RUNNING状态才有
	allocatedMB            *prometheus.Desc // 已分配的内存
	allocatedVCores        *prometheus.Desc // 已分配的Vcores
	reservedMB             *prometheus.Desc // 驻留内存
	reservedVCores         *prometheus.Desc // 驻留Vcores
	runningContainers      *prometheus.Desc // 正在运行的容器
	queueUsagePercentage   *prometheus.Desc // 使用资源占队列的百分比
	clusterUsagePercentage *prometheus.Desc // 使用资源占集群的百分比
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

// http请求，设置头并转json
func HTTPToJSON(url string) (map[string]interface{}, error) {
	t, err := strconv.Atoi(*timeout)
	client := http.Client{
		Timeout: time.Duration(t * int(time.Second)),
	}
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Transfer-Encoding", "chunked")
	res, err := client.Do(req) // 建立连接
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	var f interface{}
	err = json.Unmarshal(data, &f)
	if err != nil {
		log.Error(err)
	}
	m := f.(map[string]interface{})
	return m, nil
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
	c.activeServerIP = t.IP.String()
	// 默认关闭https
	c.HttpsOpen = httpsmode
	for _, id := range strings.Split(SearchConf("yarn.resourcemanager.ha.rm-ids", e), ",") {
		r := "yarn.resourcemanager.hostname." + id
		h := SearchConf(r, e)
		t, _ := net.ResolveIPAddr("ip", h)
		c.ResourmanagerIPList = append(c.ResourmanagerIPList, t.IP.String()) // 添加到切片中，存储RM的清单
	}
	c.activeRMID = strings.Split(SearchConf("yarn.resourcemanager.ha.rm-ids", e), ",")[0]
	// 判断是否开启HTTPS，并获取端口
	if v := SearchConf("yarn.http.policy", e); v == "HTTPS_ONLY" {
		c.HttpsOpen = true
		c.HttpsPort = strings.Split(SearchConf("yarn.resourcemanager.webapp.https.address."+c.activeRMID, e), ":")[1]
	} else {
		c.HttpPort = strings.Split(SearchConf("yarn.resourcemanager.webapp.address."+c.activeRMID, e), ":")[1]
	}
	return &c
}

func NewExporter(url string, c *YARNConf) *Exporter {
	return &Exporter{
		url: url,
		c:   *c,
		applicationState: prometheus.NewDesc(
			"application_applicationState",
			"The application state 0,1,2,3",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		startedTime: prometheus.NewDesc(
			"application_startedTime",
			"The application's  start time",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		finishedTime: prometheus.NewDesc(
			"application_finishedTime",
			"The application's  finish time",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		elapsedTime: prometheus.NewDesc(
			"application_elapsedTime",
			"The application's  elapsed time",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		memorySeconds: prometheus.NewDesc(
			"application_memorySeconds",
			"The application's memory seconds",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		vcoreSeconds: prometheus.NewDesc(
			"application_vcoreSeconds",
			"The application's vcore seconds",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		// Running applications specific
		allocatedMB: prometheus.NewDesc(
			"application_allocatedMB",
			"The application's allocated memory MB",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		allocatedVCores: prometheus.NewDesc(
			"application_allocatedVCores",
			"The application's allocated vcore",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		reservedMB: prometheus.NewDesc(
			"application_reservedMB",
			"The application's reserved vcore",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		reservedVCores: prometheus.NewDesc(
			"application_reservedVCores",
			"The application's reserved vcore",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		runningContainers: prometheus.NewDesc(
			"application_runningContainers",
			"The application's running containers",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		queueUsagePercentage: prometheus.NewDesc(
			"application_queueUsagePercentage",
			"The application's usage of queue",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
		clusterUsagePercentage: prometheus.NewDesc(
			"application_clusterUsagePercentage",
			"The application's usage of cluster",
			[]string{"applicationID", "amContainer", "applicationType", "name", "user"},
			prometheus.Labels{},
		),
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.applicationState
	ch <- e.startedTime
	ch <- e.finishedTime
	ch <- e.elapsedTime
	ch <- e.memorySeconds
	ch <- e.vcoreSeconds
	ch <- e.allocatedMB
	ch <- e.allocatedVCores
	ch <- e.reservedMB
	ch <- e.reservedVCores
	ch <- e.runningContainers
	ch <- e.queueUsagePercentage
	ch <- e.clusterUsagePercentage
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	// 实现Collect方法
	v, err := HTTPToJSON(e.url + "/ws/v1/cluster/apps?deSelects=resourceRequests&state=RUNNING,FINISHED,FAILED,KILLED")
	if err != nil {
		// 如果返回了错误，就要切换RM
		for _, ip := range e.c.ResourmanagerIPList {
			if e.c.activeServerIP != ip {
				e.c.activeServerIP = ip
				break
			}
		}
		v, err = HTTPToJSON(e.url + "/ws/v1/cluster/apps?deSelects=resourceRequests&state=RUNNING,FINISHED,FAILED,KILLED")
		if err != nil {
			log.Error(err)
			panic(1)
		}
	}
	var t = v["apps"].(map[string]interface{})["app"].([]interface{})
	for _, app := range t {
		appDataMap := app.(map[string]interface{})
		appState := -1.0
		appID := appDataMap["id"].(string)
		amContainer := strings.Split(appDataMap["amContainerLogs"].(string), "/")[5]
		appType := appDataMap["applicationType"].(string)
		name := appDataMap["name"].(string)
		user := appDataMap["user"].(string)
		if appDataMap["state"] == "RUNNING" {
			//此处，需要对RUNNING任务和其他任务进行区分
			appState = 1
			ch <- prometheus.MustNewConstMetric(
				e.allocatedMB,
				prometheus.GaugeValue,
				appDataMap["allocatedMB"].(float64),
				appID, amContainer, appType, name, user,
			)
			ch <- prometheus.MustNewConstMetric(
				e.allocatedVCores,
				prometheus.GaugeValue,
				appDataMap["allocatedVCores"].(float64),
				appID, amContainer, appType, name, user,
			)
			ch <- prometheus.MustNewConstMetric(
				e.reservedMB,
				prometheus.GaugeValue,
				appDataMap["reservedMB"].(float64),
				appID, amContainer, appType, name, user,
			)
			ch <- prometheus.MustNewConstMetric(
				e.reservedVCores,
				prometheus.GaugeValue,
				appDataMap["reservedVCores"].(float64),
				appID, amContainer, appType, name, user,
			)
			ch <- prometheus.MustNewConstMetric(
				e.runningContainers,
				prometheus.GaugeValue,
				appDataMap["runningContainers"].(float64),
				appID, amContainer, appType, name, user,
			)
			ch <- prometheus.MustNewConstMetric(
				e.queueUsagePercentage,
				prometheus.GaugeValue,
				appDataMap["queueUsagePercentage"].(float64),
				appID, amContainer, appType, name, user,
			)
			ch <- prometheus.MustNewConstMetric(
				e.clusterUsagePercentage,
				prometheus.GaugeValue,
				appDataMap["clusterUsagePercentage"].(float64),
				appID, amContainer, appType, name, user,
			)
		}
		if appDataMap["finalStatus"] == "KILLED" {
			appState = 3
		}
		if appDataMap["finalStatus"] == "SUCCEEDED" {
			appState = 0
		}
		if appDataMap["finalStatus"] == "FAILED" {
			appState = 2
		}
		// 其实我觉得用switch也行
		ch <- prometheus.MustNewConstMetric(
			e.applicationState,
			prometheus.GaugeValue,
			appState,
			appID, amContainer, appType, name, user,
		)
		ch <- prometheus.MustNewConstMetric(
			e.startedTime,
			prometheus.GaugeValue,
			appDataMap["startedTime"].(float64),
			appID, amContainer, appType, name, user,
		)
		ch <- prometheus.MustNewConstMetric(
			e.finishedTime,
			prometheus.GaugeValue,
			appDataMap["finishedTime"].(float64),
			appID, amContainer, appType, name, user,
		)
		ch <- prometheus.MustNewConstMetric(
			e.elapsedTime,
			prometheus.GaugeValue,
			appDataMap["elapsedTime"].(float64),
			appID, amContainer, appType, name, user,
		)
		ch <- prometheus.MustNewConstMetric(
			e.memorySeconds,
			prometheus.GaugeValue,
			appDataMap["memorySeconds"].(float64),
			appID, amContainer, appType, name, user,
		)
		ch <- prometheus.MustNewConstMetric(
			e.vcoreSeconds,
			prometheus.GaugeValue,
			appDataMap["vcoreSeconds"].(float64),
			appID, amContainer, appType, name, user,
		)
	}
}

func main() {
	flag.Parse()
	log.Info("Application Exporter make By Lijiadong(Meepod) (๑•̀ㅂ•́)و✧")
	conf := CreateYARNConf(ReadXml(*clientConfFile))
	resourcemanagerURL := "http://" + conf.activeServerIP + ":" + conf.HttpPort
	if conf.HttpsOpen {
		resourcemanagerURL = "https://" + conf.activeServerIP + ":" + conf.HttpsPort
	}
	exporter := NewExporter(resourcemanagerURL, conf)
	prometheus.MustRegister(exporter)
	log.Info("Starting Server: %s", *listenAddress)
	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
		<head><title>Applications Exporter</title></head>
		<body>
		<h1>Applications Exporter By Meepo</h1>
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
