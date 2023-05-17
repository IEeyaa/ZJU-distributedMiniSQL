package Components;

import java.net.ServerSocket;
import java.net.Socket;
import INTERPRETER.API;

public class Region {
    static final String ZookeeperIP = "127.0.0.1";
    static final int ZookeeperPort = 12345;
    static String endCode = "$end";
    static public MasterThread masterThread;
    static int regionListenPort = 8080;
    static String ip = "127.0.0.1";

    public Region(String ip, int port) {
        Region.ip = ip;
        Region.regionListenPort = port;
    }

    public void run() throws Exception {
        API.initial();
        System.out.println("hello, Welcome to REGION & minisql~");
        // 主动连接Zookeeper
        new Thread(new ZookeeperThread(ZookeeperIP, ZookeeperPort, regionListenPort)).start();
        // 测试用
        // Region.masterThread = new MasterThread("127.0.0.1", 8086, regionListenPort);
        // new Thread(Region.masterThread).start();
        // 监听端口
        try (ServerSocket serverSocket = new ServerSocket(regionListenPort)) {
            // 每当出现新的连接，则建立一个线程来处理
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new ClientThread(socket)).start();
            }
        }
    }
}
