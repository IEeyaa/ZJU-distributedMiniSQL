package Components;

import java.io.*;
import java.net.Socket;
import Connection.Connect;
import java.util.Timer;
import java.util.TimerTask;
import INTERPRETER.Interpreter;

public class MasterThread implements Runnable {

    public Connect master_connector;
    private Connect zookeeper_connector;
    private static long alive_time = 10000L;
    private int ZookeeperPort;
    private String ZookeeperIP;
    private int port;
    private String ip;
    private int regionListenPort;

    // 结尾符
    static String endCode = "";

    public MasterThread(String ip, int port, int regionListenPort) {
        this.ZookeeperIP = ip;
        this.ZookeeperPort = port;
        this.regionListenPort = regionListenPort;
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
                    if (parts[0] == "null") {
                        continue;
                    }
                    this.ip = parts[0];
                    this.port = Integer.parseInt(parts[1]);
                    zookeeper_connector.close();
                    break;
                }
            }
            System.out.println(String.format("master ip get %s:%d", ip, port));
            master_connector = new Connect(ip, port, "master");
            master_connector.connect();
            System.out.println("connect master OK");
            master_connector.send(String.format(("(hello)%d"), regionListenPort));

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

            // 接受请求、发送请求【create drop insert delete】
            while (true) {
                String result = master_connector.receive();
                if (result != null) {
                    String[] parts = result.split("(?<=\\D)(?=\\d)");
                    if (parts.length < 2) {
                        continue;
                    }
                    String method = parts[0]; // create
                    String infor = parts[1];
                    switch (method) {
                        case "copy":
                            String[] addresses = infor.split(":");
                            copy_from_table(addresses[0], Integer.parseInt(addresses[1]), addresses[2]);
                            break;
                        case "sql":
                            String main_sentence = infor.trim().replaceAll("\\s+", " ");
                            // to minisql
                            Interpreter.interpret(main_sentence);
                            break;
                        default:
                            break;
                    }
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void copy_from_table(String ip, int port, String tableName) {
        System.out.println(String.format("copy from %s:%d tablename is %s", ip, port, tableName));
        // 创建一个新文件
        File file = new File(tableName);
        if (!file.createNewFile()) // file already exists
            throw new NullPointerException();
        // 可以成功创建，则建立一个Connect连接
        Connect region_connector = new Connect(ip, port, "region");
        // 发送copy请求
        region_connector.send("copy:" + tableName);
        // 收到回复
        String result = region_connector.receive();
        try (FileWriter writer = new FileWriter(file)) {
            String temp = "";
            writer.write(temp);
        } catch (IOException e) {
            e.printStackTrace();
        }
        region_connector.close();
    }
}