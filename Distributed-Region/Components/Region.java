package Components;

import java.net.ServerSocket;
import java.net.Socket;

import Connection.Connect;
import INTERPRETER.API;

public class Region {
    static String MasterIP = "10.181.194.248";
    static int masterPort = 8086;
    static String ZookeeperIP = "127.0.0.1:2181";
    public static Connect master_connector;
    static String endCode = "$end";

    static int regionPort = 8080;
    static String ip = "127.0.0.1";

    public Region(String ip, int port) {
        Region.ip = ip;
        Region.regionPort = port;
    }

    public void run() throws Exception {
        API.initial();
        System.out.println("hello, Welcome to region & minisql~");
        master_connector = new Connect(MasterIP, masterPort, "master");
        master_connector.connect();
        master_connector.send("hello");
        try (ServerSocket serverSocket = new ServerSocket(regionPort)) {
            // 每当出现新的连接，则建立一个线程来处理
            while (true) {
                // 监听
                // master_preload();
                Socket socket = serverSocket.accept();
                new Thread(new RegionThread(socket, "client")).start();
                // String result = master_connector.receive();
                // if (result != null) {
                // System.out.println(result);
                // }
            }
        }
    }

    public void master_preload() {
        while (true) {
            String result = master_connector.receive();
            if (result != null) {
                System.out.println(result);
            }
        }
    }
}
