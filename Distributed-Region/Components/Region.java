package Components;

import java.net.ServerSocket;
import java.net.Socket;
import INTERPRETER.API;

public class Region {
    static String MasterIP = "127.0.0.1";
    static int masterPort = 8086;
    static String ZookeeperIP = "127.0.0.1:2181";
    public static RegionThread MasterThread;
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
        try (ServerSocket serverSocket = new ServerSocket(regionPort)) {
            // 每当出现新的连接，则建立一个线程来处理
            while (true) {
                Socket socket = serverSocket.accept();
                if (socket.getPort() != masterPort) {
                    new Thread(new RegionThread(socket, "client")).start();
                } else {
                    new Thread(new RegionThread(socket, "master")).start();
                }
            }
        }
    }
}
