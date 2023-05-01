# Distributed-Client

孔郁杰

## 第一阶段

+ 获取请求中的表，检索缓存
  + 存在，发送给Region
  + 不存在，发送给Master，得到回复，更新缓存，发送给Region

+ 获取Region回复，显示

