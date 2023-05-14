import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.net.ServerSocket;
import java.net.Socket;

public class ZooKeeper {
    static String masterIp = null;
    static int masterPort = -1;
    static int port = 12345;

    public static void main(String[] args) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("ZooKeeperServer is running on port " + port);
            while (true) {
                // 监听端口
                Socket socket = serverSocket.accept(); // 等待客户端连接请求
                // IO请求
                BufferedReader in = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
                // 消息打印
                System.out.println("Received a connection from " + socket.getInetAddress());
                String request = in.readLine();
                System.out.println("Received request: " + request);
                if (request.startsWith("client")) {
                    // 分配给client线程处理
                    new Thread(new ClientThread(socket, out)).start();
                } else if (request.startsWith("region")) {
                    // 分配给region线程处理
                    new Thread(new RegionThread(socket)).start();
                } else {
                    System.out.println("invalid request");
                    socket.close();
                }
            }
        }
    }
}
