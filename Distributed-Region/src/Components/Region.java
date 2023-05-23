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
    // 标识region是否可以正常工作
    static boolean alive = true;

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
        // 监听端口
        boolean portAvailable = false;
        ServerSocket serverSocket = null;

        while (!portAvailable) {
            try {
                serverSocket = new ServerSocket(regionListenPort);
                portAvailable = true;
            } catch (IOException e) {
                regionListenPort++; // 自增端口号
            }
        }
        // 打印欢迎信息
        System.out.println(
                String.format("HELLO, Welcome to REGION & minisql~ the region listening port is %d\n",
                        regionListenPort));

        // 主动连接Zookeeper
        new Thread(new ZookeeperThread(ZookeeperIP, ZookeeperPort, regionListenPort)).start();

        try {
            // 每当出现新的连接，则建立一个线程来处理
            while (alive) {
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
