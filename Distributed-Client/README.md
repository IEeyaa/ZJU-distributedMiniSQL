# Distributed-Client

孔郁杰

## 第一阶段（5.3）

##### 工作流程：

- 与region建立连接
  - 如果是create，向Master发送create请求，获得Region，然后将SQL发送给Region
  - 如果是其他SQL，获取请求中的表名，检索缓存
    - 存在，发送SQL给Region
    - 不存在，发送表名给Master，得到回复，更新缓存，发送SQL给Region

- 获取Region回复，显示

##### 通信规范：

- client &rarr; master

  |       req       |              res               |
  | :-------------: | :----------------------------: |
  |     \<quit>     |              null              |
  |    \<create>    |            ip:port             |
  | \<get>tablename |            ip:port             |
  |\<show>tablename | {<br>table1,<br/>table2,<br/>} |

- client &rarr; region

  |   req    |  res   |
  | :------: | :----: |
  |   SQL    | result |


- client &rarr; zookeeper

|    req    |   res   |
| :-------: | :-----: |
| client | ip:port |
