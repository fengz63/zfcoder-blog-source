---
title: client-go 源码解读（一）总述
date: 2025-08-18 13:37:33
tags: 
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 背景
最近工作中频繁用到 Informer、Controller 等相关组件，以及 List、Watch 等 API，为了更好地理解其工作原理，决定对 client-go 源码进行详细解读。

<!--more-->

在开始源码解读之前，先简单介绍一下 client-go 一些相关的结构，示意图如下：

![](https://cdn.jsdelivr.net/gh/fengz63/picture@main/2025/08/18/18210845.jpeg)

整个架构图分为上下两部分，其中上半部分为 client-go 的实现，而下半部分是我们自己要实现的 custom controller，每部分由不同的组件组成，上下两部分通过虚线连接起来。我们着重介绍下上半部分的组件，后续的源码解读也是基于上半部分架构图进行的。
- **Reflector**：通过 List&Watch 机制从 kube-apiserver 获取资源对象，并把数据放到 Delta FIFO 队列中；
- **DeltaFIFO**：Delta FIFO 是一个先进先出队列，用于存储 Reflector 从 kube-apiserver 获取到的资源对象的增量变化
- **Informer**：Informer 从 DeltaFIFO 中取出相应对象，然后通过 Indexer 将对象和索引丢到放入 cache 中，再触发相应的事件处理函数（Resource Event Handlers）运行；
- **Indexer**：主要提供一个对象根据一定条件检索的能力，典型的是通过 namespace/name 来构造 key，通过 ThreadSafeStore 来存储对象
- **Workqueue**：Workqueue 一般使用的是延时队列实现，在 Resource Event Handlers 中会完成将对象的 key 放入 workqueue 的过程，然后我们在自己的逻辑代码里从 workqueue 中消费这些 key（架构图下半部分）；
- **Resource Event Handlers**：Resource Event Handlers 是 Informer 注册的回调函数，当 Informer 从 DeltaFIFO 中取出对象后，会根据对象的操作类型（Add/Update/Delete）调用对应的回调函数；
