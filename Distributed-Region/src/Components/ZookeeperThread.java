package Components;

import Connection.Connect;

// 用于连接zookeeper，汇报监听端口，同时持续监听zookeeper信息（广播/任命）
public class ZookeeperThread implements Runnable {

    private int ZookeeperPort;
    private String ZookeeperIP;
    private int regionListenPort;
    private Connect zookeeper_connector;

    public ZookeeperThread(String ip, int port, int regionListenPort) {
        this.ZookeeperIP = ip;
        this.ZookeeperPort = port;
        this.regionListenPort = regionListenPort;
    }

    public void run() {
        try {
            zookeeper_connector = new Connect(ZookeeperIP, ZookeeperPort, "zookeeper");
            if (!zookeeper_connector.connect()) {
                System.out.println("no zookeeper, sorry");
                System.exit(1);
            }
            System.out.println("<zookeeper>connect zookeeper OK");
            zookeeper_connector.send("region:" + regionListenPort);
            while (true) {
                String result = zookeeper_connector.receive();
                if (result != null) {
                    // 打印Zookeeper信息
                    System.out.println("<zookeeper>" + result);
                    // 更换Master节点, 数据格式master_change:ip:port
                    if (result.startsWith("change")) {
                        String[] parts = result.split(":");
                        Region.masterThread = new MasterThread(parts[1], Integer.parseInt(parts[2]), regionListenPort);
                        new Thread(Region.masterThread).start();
                    }
                    // 成为新的master, 数据格式master
                    else if (result.startsWith("toMaster")) {
                        System.out.println("I'm the new master");
                        // 处理其它事情
                        // 1.new一个master线程
                        // 2.new的时候利用CatalogManager.get_table()获取表信息并传入构造函数
                        // 3.杀掉以下线程
                        // 自己线程一定要杀
                        // Region.masterThread 杀掉
                    }
                    // 初始连接
                    else {
                        String[] parts = result.split(":");
                        if (parts[0] == "null") {
                            continue;
                        }
                        Region.masterThread = new MasterThread(parts[0], Integer.parseInt(parts[1]), regionListenPort);
                        new Thread(Region.masterThread).start();
                        zookeeper_connector.socket.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
