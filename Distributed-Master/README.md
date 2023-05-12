# 二阶段任务

+ 处理quit
+ 通信规范（hello）

region->master

(create)tablename

(drop)tablename

+ 下阶段目标

show tables;

多条语句分割

心跳

加入zookeeper

process

# 三阶段

### 通信规范

master->region:

+ (copy)ip:port:tablename
+ (sql)statement

region->master:

+ (modify)statement

master->zookeeper:

+ (remove)ip:port
