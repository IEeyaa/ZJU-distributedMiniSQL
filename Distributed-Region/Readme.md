## Region开发板块

by 孙宇桐

#### 一阶段

##### 工作流程

+ 和Master建立连接【Master端口确定为8086】
  + 主动建立socket连接
  + 发送一个(Hello)，使得Master能够读取信息
  + 持续接受Master信息并且回显
+ 和Client建立连接【Client端口随机，Region端口确定为8080】
  + 被动监听8080端口
  + 如果有Client加入连接，将socket分配到一个线程处理

##### 通信规范

Client to Region

| req         | res            |
| ----------- | -------------- |
| SQL(String) | result(String) |

Region to Master

| req           | res  |
| ------------- | ---- |
| (hello)       | null |
| (Create/Drop) | null |



#### 二阶段

##### 工作流程

Client to Region(√)【表相关操作】

+ Create
+ Drop, Select, Insert, Delete
+ Show

Master to Region

+ 建表删表

  + R-M:(CREATE), (DROP)

+ 心跳

  + M-R(DETECT)
  + R-M(ALIVE) 或者 (ERROR)

+ > 查表
  >
  > + M-R(SHOWTABLE)
  > + R-M(TABLES)table1:table2:table3: ... :tablen

**通信规范**

Master to Region

| req         | res                                       |
| ----------- | ----------------------------------------- |
| (Detect)    | (Alive) or (Wrong)                        |
| (Showtable) | (TABLES)table1:table2:table3: ... :tablen |

