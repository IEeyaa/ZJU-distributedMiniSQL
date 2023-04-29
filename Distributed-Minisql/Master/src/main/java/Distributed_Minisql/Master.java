package Distributed_Minisql;

import java.io.IOException;

public class Master {
    // public ZookeeperManager zookeeperManager;
    // public TableManager tableManager;
    // private final SocketManager socketManager;
    private Table table;
    private Listener listener;

    private final int PORT = 8086;

    public Master() throws IOException, InterruptedException {
        // 新建一个Master中所有服务共享的表管理器
        // tableManager = new TableManager();
        // zookeeperManager = new ZookeeperManager(tableManager);
        // socketManager = new SocketManager(PORT, tableManager);
        table = new Table();
        listener = new Listener(table);
    }

    public void start() throws IOException, InterruptedException{
        // 用于实时监控zookeeper下的zNode信息的线程
        // Thread monitor = new Thread(zookeeperManager);
        // monitor.start();
        // 负责和从节点通信的线程
        // socketManager.startSocketManager();
        listener.startListen(PORT);
    }
}
