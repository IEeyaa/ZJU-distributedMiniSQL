package Components;

import java.io.*;
import java.net.Socket;
import Connection.Connect;
import java.util.Timer;
import java.util.TimerTask;

public class MasterThread implements Runnable {

    private Connect master_connector, zookeeper_connector;
    private static long alive_time = 1000L;
    private int ZookeeperPort;
    private String ZookeeperIP;
    private int port;
    private String ip;

    // 结尾符
    static String endCode = "";

    public MasterThread(String ip, int port) {
        this.ZookeeperIP = ip;
        this.ZookeeperPort = port;
    }

    public void run() {
        try {
            zookeeper_connector = new Connect(ZookeeperIP, ZookeeperPort, "zookeeper");
            zookeeper_connector.connect();
            System.out.println("connect zookeepeer OK");
            zookeeper_connector.send("region");
            while (true) {
                String result = zookeeper_connector.receive();
                if (result != null) {
                    System.out.println(result);
                    String[] parts = result.split(":");
                    this.ip = parts[0];
                    this.port = Integer.parseInt(parts[1]);
                    zookeeper_connector.close();
                    break;
                }
            }
            master_connector = new Connect(ip, port, "master");
            master_connector.connect();
            master_connector.send("hello");

            // 创建定时器
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    // 每隔10秒发送一次"alive"
                    master_connector.send("(ALIVE)");
                    System.out.println("OK");
                }
            }, 0L, alive_time);

            // 接受请求
            while (true) {
                String result = master_connector.receive();
                if (result != null) {
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}