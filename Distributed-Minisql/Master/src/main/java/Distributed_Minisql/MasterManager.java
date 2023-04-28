package Distributed_Minisql;

import java.io.IOException;

public class MasterManager {
    public ZookeeperManager zookeeperManager;
    public TableManager tableManager;
    private final SocketManager socketManager;

    private final int PORT = 8086;

    public MasterManager() throws IOException, InterruptedException {
        // 新建一个Master中所有服务共享的表管理器
        tableManager = new TableManager();
        zookeeperManager = new ZookeeperManager(tableManager);
        socketManager = new SocketManager(PORT, tableManager);
    }

    public void startUp() throws IOException, InterruptedException{
        // 用于实时监控zookeeper下的zNode信息的线程
        Thread monitor = new Thread(zookeeperManager);
        monitor.start();
        // 负责和从节点通信的线程
        socketManager.startSocketManager();
    }
}
