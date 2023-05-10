package Components;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import Connection.Connect;
import INTERPRETER.API;

public class Region {
    static final String ZookeeperIP = "10.162.90.213";
    static final int ZookeeperPort = 12345;
    static String endCode = "$end";

    static int regionPort = 8080;
    static String ip = "127.0.0.1";

    public Region(String ip, int port) {
        Region.ip = ip;
        Region.regionPort = port;
    }

    public void run() throws Exception {
        API.initial();
        // System.out.println("input a port number:\n");
        // try (Scanner sc = new Scanner(System.in)) {
        // regionPort = sc.nextInt();
        // }
        System.out.println("hello, Welcome to region & minisql~");
        new Thread(new MasterThread(ZookeeperIP, ZookeeperPort)).start();
        try (ServerSocket serverSocket = new ServerSocket(regionPort)) {
            // 每当出现新的连接，则建立一个线程来处理
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new RegionThread(socket, "client")).start();
            }
        }
    }
}