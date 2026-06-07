---
title: Go 类型分析
date: 2026-01-18 10:47:00
tags: 技术
categories: [技术]
---

Golang 是一门静态类型语言，变量的类型决定了它在内存中的存储方式以及赋值、传参行为。在 Go 中，类型大体可以分为 **值类型（Value Types）** 和 **引用类型（Reference Types）**。理解它们的区别，对于编写高效、正确的 Go 代码非常关键。

<!-- more -->

## 值类型（Value Types）

值类型在赋值或作为函数参数传递时，会**复制一份独立的内存**。对副本的修改不会影响原变量。

### 常见的值类型

- 基本类型：int，float64，bool，string
- 数组：array
- 结构体：struct

### 示例

```go
package main

import "fmt"

type Point struct {
    X, Y int
}

func main() {
    a := 10
    b := a  // b 拷贝了一份 a 的值
    b = 20
    fmt.Println(a, b) // 10 20

    p1 := Point{X: 1, Y: 2}
    p2 := p1 // p2 拷贝了一份 p1 的结构体内容
    p2.X = 100
    fmt.Println(p1, p2) // {1 2} {100 2}
}
```

其中，a 和 b 是两个独立的整数变量，修改 b 不影响 a。p1 和 p2 是两个独立的结构体拷贝，修改 p2.X 不影响 p1.X。

### 使用场景

- 结构简单、数据量小的类型
- 不希望函数修改原值时
- 提高代码的可读性和安全性

## 引用类型

引用类型在赋值或传参时，变量存储的是**底层数据的地址**，多个变量可以指向同一块内存。修改其中一个变量的数据，会影响到所有引用。

### 常见的引用类型

- 切片：slice
- 映射：map
- 通道：channel
- 指针：pointer
- 函数：func

### 示例

```go
package main

import "fmt"

func main() {
    s1 := []int{1, 2, 3}
    s2 := s1 // s2 和 s1 指向同一块底层数组
    s2[0] = 100
    fmt.Println(s1, s2) // [100 2 3] [100 2 3]

    m1 := map[string]int{"a": 1}
    m2 := m1 // m2 和 m1 指向同一个 map
    m2["a"] = 999
    fmt.Println(m1, m2) // map[a:999] map[a:999]
}
```

- 切片 s1 和 s2 的底层数组共享，修改任意一个会影响另一个。
- map 也是引用类型，多次赋值只复制了引用。
- 指针直接操作底层内存，实现引用传递。

### 使用场景

- 大对象或集合，不想复制大量数据
- 函数需要修改原始数据
- 数据需要在不同作用域共享

## 值类型 VS 引用类型的总结对比

| 特性 | 值类型 | 引用类型 |
|:---|:---|:---|
| 内存复制 | 复制整个值 | 复制引用 |
| 修改是否影响原变量 | 否 | 是 |
| 常见类型 | int, float, bool, struct, array | slice, map, channel, pointer, func |
| 性能 | 小对象性能好 | 大对象节省内存，避免复制 |
