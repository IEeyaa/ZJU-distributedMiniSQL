package Components;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import INTERPRETER.API;

public class Region implements Runnable {
    static String ZookeeperIP = "127.0.0.1";
    static int ZookeeperPort = 12345;
    static String endCode = "$end";
    static public List<ClientThread> threads = new ArrayList<>();
    static public MasterThread masterThread;
    static int regionListenPort = 8080;

    public Region(String ip, int port, int listenPort) {
        Region.ZookeeperIP = ip;
        Region.ZookeeperPort = port;
        Region.regionListenPort = listenPort;
    }

    public void run() {
        try {
            API.initial();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                ClientThread clientThread = new ClientThread(socket);
                threads.add(clientThread);
                new Thread(clientThread).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
