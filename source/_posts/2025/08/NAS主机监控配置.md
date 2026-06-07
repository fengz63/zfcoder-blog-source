---
title: Ubuntu 24.04 配置 Prometheus 和 Grafana 监控
date: 2025-08-10 07:23:47
tags: 技术
categories: [技术]
---

最近工作中使用了 prometheus 数据采集搭配 grafana 监控，感觉监控使用很方便。于是趁着周末给自己家里的NAS上装了一个prometheus数据，在用grafana配合完成对个人NAS的性能监控。

<!-- more -->

## 在宿主机上安装 Prometheus
### 安装
Ubuntu 默认的源里面就有 prometheus 的安装包，因此安装命令：
```bash
sudo apt install prometheus
```
检查 prometheus 相关服务是否启动：
```bash
# systemctl status prometheus
● prometheus.service - Monitoring system and time series database
     Loaded: loaded (/usr/lib/systemd/system/prometheus.service; enabled; preset: enabled)
     Active: active (running) since Sun 2025-08-10 09:50:12 CST; 5h 4min ago
       Docs: https://prometheus.io/docs/introduction/overview/
             man:prometheus(1)
   Main PID: 1270424 (prometheus)
      Tasks: 11 (limit: 9223)
     Memory: 111.7M (peak: 148.7M)
        CPU: 2min 2.878s
     CGroup: /system.slice/prometheus.service
             └─1270424 /usr/bin/prometheus

8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.181Z caller=compact.go:523 level=info component=tsdb msg="write block" >
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.188Z caller=head.go:1293 level=info component=tsdb msg="Head GC complet>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.391Z caller=compact.go:464 level=info component=tsdb msg="compact block>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.397Z caller=db.go:1617 level=info component=tsdb msg="Deleting obsolete>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.402Z caller=db.go:1617 level=info component=tsdb msg="Deleting obsolete>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.406Z caller=db.go:1617 level=info component=tsdb msg="Deleting obsolete>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.714Z caller=compact.go:464 level=info component=tsdb msg="compact block>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.722Z caller=db.go:1617 level=info component=tsdb msg="Deleting obsolete>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.729Z caller=db.go:1617 level=info component=tsdb msg="Deleting obsolete>
8月 10 13:00:05 zfeng prometheus[1270424]: ts=2025-08-10T05:00:05.736Z caller=db.go:1617 level=info component=tsdb msg="Deleting obsolete>
```
可以看到prometheus的控制端口是9090。此时在浏览器中访问服务器ip和9090端口，如果 prometheus 页面可以正常打开就说明安装完成：

![](/images/prometheus_web_ui.png)

## 在被监控节点安装 node-exporter
也是直接从ubuntu源安装
```bash
sudo apt install prometheus-node-exporter
```
检查 node-exporter 服务有没有启动
```bash
# systemctl status prometheus-node-exporter
● prometheus-node-exporter.service - Prometheus exporter for machine metrics
     Loaded: loaded (/usr/lib/systemd/system/prometheus-node-exporter.service; enabled; preset: enabled)
     Active: active (running) since Sat 2025-08-09 18:30:39 CST; 20h ago
       Docs: https://github.com/prometheus/node_exporter
   Main PID: 1221924 (prometheus-node)
      Tasks: 6 (limit: 9223)
     Memory: 10.6M (peak: 12.2M)
        CPU: 24min 7.150s
     CGroup: /system.slice/prometheus-node-exporter.service
             └─1221924 /usr/bin/prometheus-node-exporter "--collector.filesystem.ignored-mount-points=^/(sys|proc|dev|run)(\$|/)" "--coll>

8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=the>
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=time
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=tim>
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=udp>
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=una>
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=vms>
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=xfs
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.940Z caller=node_exporter.go:117 level=info collector=zfs
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.941Z caller=tls_config.go:313 level=info msg="Listening o>
8月 09 18:30:39 zfeng prometheus-node-exporter[1221924]: ts=2025-08-09T10:30:39.941Z caller=tls_config.go:316 level=info msg="TLS is disa>
```
可以看到 node-exporter 监控的端口号是 9100。

## 配置监控机的 prometheus.xml 加入被监控的节点
打开`/etc/prometheus/prometheus.yml`文件，编辑对应位置，加入节点配置，如下：
![](/images/prometheus_server_config.png)
然后重启服务：
```bash
systemctl restart prometheus
```
再打开 prometheus 的监控列表，发现已有两个节点被监控了，包括监控机本身：

![](/images/prometheus_web_ui_2.png)

## 安装 Grafana
Ubuntu 默认源中没有 grafana 的安装包，因此需要添加对应的源。国内可以用清华源代替，详细见：[添加 Grafana 软件仓库](https://mirrors.tuna.tsinghua.edu.cn/help/grafana/)

安装完后检查服务是否启动：
```bash
# systemctl status grafana-server
● grafana-server.service - Grafana instance
     Loaded: loaded (/usr/lib/systemd/system/grafana-server.service; disabled; preset: enabled)
     Active: active (running) since Sat 2025-08-09 18:00:09 CST; 21h ago
       Docs: http://docs.grafana.org
   Main PID: 1219957 (grafana)
      Tasks: 19 (limit: 9223)
     Memory: 122.9M (peak: 158.3M)
        CPU: 11min 21.470s
     CGroup: /system.slice/grafana-server.service
             └─1219957 /usr/share/grafana/bin/grafana server --config=/etc/grafana/grafana.ini --pidfile=/run/grafana/grafana-server.pid >

8月 10 15:00:22 zfeng grafana[1219957]: logger=cleanup t=2025-08-10T15:00:22.049771001+08:00 level=info msg="Completed cleanup jobs" dura>
8月 10 15:00:23 zfeng grafana[1219957]: logger=plugins.update.checker t=2025-08-10T15:00:23.431751891+08:00 level=info msg="Update check >
8月 10 15:01:28 zfeng grafana[1219957]: logger=infra.usagestats t=2025-08-10T15:01:28.986667313+08:00 level=info msg="Usage stats are rea>
8月 10 15:10:22 zfeng grafana[1219957]: logger=cleanup t=2025-08-10T15:10:22.020334022+08:00 level=info msg="Completed cleanup jobs" dura>
8月 10 15:10:23 zfeng grafana[1219957]: logger=plugins.update.checker t=2025-08-10T15:10:23.51111745+08:00 level=info msg="Update check s>
8月 10 15:20:22 zfeng grafana[1219957]: logger=cleanup t=2025-08-10T15:20:22.022565084+08:00 level=info msg="Completed cleanup jobs" dura>
8月 10 15:20:23 zfeng grafana[1219957]: logger=plugins.update.checker t=2025-08-10T15:20:23.415660356+08:00 level=info msg="Update check >
8月 10 15:30:22 zfeng grafana[1219957]: logger=cleanup t=2025-08-10T15:30:22.047368654+08:00 level=info msg="Completed cleanup jobs" dura>
8月 10 15:30:23 zfeng grafana[1219957]: logger=plugins.update.checker t=2025-08-10T15:30:23.469371479+08:00 level=info msg="Update check >
8月 10 15:31:28 zfeng grafana[1219957]: logger=infra.usagestats t=2025-08-10T15:31:28.98688322+08:00 level=info msg="Usage stats are read>
```
默认情况下是已经启动的，如果没启动，手动启动就好。grafana 服务默认监控端口是 3000，从浏览器中输入 ip:3000，如果能显示 grafana 页面表示一切正常。

## 配置 Grafana
Grafana 默认的账号和密码都是 admin，先登录。
### 添加数据源
打开 Web UI，进入：`Connections->Data Sources`，点击 `Add new data source` 按钮:
![](/images/grafana_1.png)
选择 Prometheus：
![](/images/grafana_2.png)
修改 Connection 选项中对应的地址，改为 prometheus server 对应的地址即可。
![](/images/grafana_3.png)

### 添加 Dashboard
Dashboard 添加有现成的模版，可以去这里找：[https://grafana.com/grafana/dashboards/?search=8919](https://grafana.com/grafana/dashboards/?search=8919)。

![](/images/grafana_dashboard.png)

选择 Linux 主机详情下载对应的json文件即可。

下载完成后在 grafana Web UI 中`New->Import`选择刚才下载的 json 文件即可得到一个比较完善的 Linux 主机详情监控面板了：

![](/images/grafana_dashboard_2.png)