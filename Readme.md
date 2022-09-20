# grpclb2etcd - 一个 grpc 的负载均衡解析器实现

## ![image](lb.png)

# 说明

关于 gRPC-LB 的实现，可以参考下比较基础的文章：</br>
[gRPC 服务发现 & 负载均衡](https://segmentfault.com/a/1190000008672912) </br>
[gRPC Load Balancing](https://grpc.io/blog/loadbalancing/) </br>

本项目借助于 gRPC 与 ETCDV3 实现了基础的服务注册与服务发现, 借助于 gRPC 的 `resolver/balancer` 包提供的接口, 实现了自定义的服务发现与负载均衡逻辑, 具体实现的思路如下:

## 服务注册

## 服务发现

## 负载均衡算法

- 带权重的 roundrobin 算法
- random 算法
- ketama 算法
- P2C 算法

## 服务访问

## 测试