# hadoop-exporter
使用Golang尝试写的hadoop-exporter

构建方式

```
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/log
go build namenode-exporter.go
go build resourcemanager-exporter.go
go build datanode-exporter.go
go build applications-exporter.go
```

Help on flags of namenode-exporter:

```
-hdfs-site.path string
       (default "/etc/hadoop/conf/hdfs-site.xml")
-log.level value
      Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal, panic].
-web.listen-address string
      暴露指标的监听地址，默认9070. (default ":9070")
-web.telemetry-path string
      暴露指标的路由. (default "/metrics")
```

Help on flags of resourcemanager-exporter:

```
-get.timeout-seconds string
      请求超时的时间 (default "5")
-log.level value
      Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal, panic].
-web.listen-address string
      暴露指标的监听地址，默认9075. (default ":9075")
-web.telemetry-path string
      暴露指标的路由. (default "/metrics")
-yarn-site.path string
      (default "/etc/hadoop/conf/yarn-site.xml")
```

Help on flags of datanode-exporter:

```
-hdfs-site.path string
       (default "/etc/hadoop/conf/hdfs-site.xml")
-log.level value
      Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal, panic].
-web.listen-address string
      暴露指标的监听地址，默认9071. (default ":9071")
-web.telemetry-path string
      暴露指标的路由. (default "/metrics")
```

Help on flags of applications-exporter:

```
-get.timeout-seconds string
      请求超时的时间 (default "5")
-log.level value
      Only log messages with the given severity or above. Valid levels: [debug, info, warn, error, fatal, panic].
-web.listen-address string
      暴露指标的监听地址，默认9077. (default ":9077")
-web.telemetry-path string
      暴露指标的路由. (default "/metrics")
-yarn-site.path string
        YARN的客户端配置路径，支持绝对路径和相对路径 (default "/etc/hadoop/conf/yarn-site.xml")
```


基于HDP3.1测试通过。
