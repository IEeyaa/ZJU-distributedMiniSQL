package components;

import java.io.IOException;

public class Master {
    // public ZookeeperManager zookeeperManager;
    // public TableManager tableManager;
    // private final SocketManager socketManager;
    private Table table;
    private Listener listener;

    private final int PORT = 8086;
    private final String ZookeeperIP = "10.162.90.213";
    private final int ZookeeperPort = 12345;

    public Master() throws IOException, InterruptedException {
        // 新建一个Master中所有服务共享的表管理器
        // tableManager = new TableManager();
        // zookeeperManager = new ZookeeperManager(tableManager);
        // socketManager = new SocketManager(PORT, tableManager);
        table = new Table();
        // table.addRegion("10.192.92.22:8080");
        listener = new Listener(table);
    }

    public void start() throws IOException, InterruptedException{
        // 用于实时监控zookeeper下的zNode信息的线程
        // Thread monitor = new Thread(zookeeperManager);
        // monitor.start();
        // 负责和从节点通信的线程
        // socketManager.startSocketManager();
        ZookeeperThread zookeeper = new ZookeeperThread(ZookeeperIP, ZookeeperPort);
        zookeeper.start();
        listener.startListen(PORT);
    }
}
