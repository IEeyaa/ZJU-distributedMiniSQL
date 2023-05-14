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

##### 通信规范

Master to Region

| req         | res                                       |
| ----------- | ----------------------------------------- |
| (Detect)    | (Alive) or (Wrong)                        |
| (Showtable) | (TABLES)table1:table2:table3: ... :tablen |

#### 三阶段

##### 工作流程

实现主从备份

+ Region在每次Create表的时候要发送给MasterCreate请求，Master收到Create请求后会向Region的从节点发送一个copy请求，使其更新该表信息
+ 新的Region加入并成为某个Region的从节点时，Master需要向其发送copy请求
+ Drop/Insert/delete请求在Region主节点出现时，Region需要向Master同步SQL语句

实现容错容灾

+ Region需要有主从标识，并且可以随时转换
+ Region开始可以被选举为Master

##### 通信规范

Master to Region

| req                     | res      |
| ----------------------- | -------- |
| (copy)ip:port:tablename | (copyok) |
| (sql)sqlstatement       | null     |

Region to Master

| req                  | res  |
| -------------------- | ---- |
| (modify)sqlstatement | null |

