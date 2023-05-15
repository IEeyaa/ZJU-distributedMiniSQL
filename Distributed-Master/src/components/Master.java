package components;

import java.io.IOException;

public class Master {
    private Table table;
    private Listener listener;
    private ZookeeperThread zookeeper;

    private final int PORT = 8086;
    private final String ZookeeperIP = "192.168.43.95";
    private final int ZookeeperPort = 12345;

    public Master() throws IOException, InterruptedException {
        zookeeper = new ZookeeperThread(ZookeeperIP, ZookeeperPort);
        table = new Table(zookeeper);
        // table.addRegion("10.192.92.22:8080");
        listener = new Listener(table);
    }

    public void start() throws IOException, InterruptedException{
        zookeeper.start();
        listener.startListen(PORT);
    }
}
