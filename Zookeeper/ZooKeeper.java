import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ZooKeeper {
    // zookeeper监听端口
    static int port = 12345;
    // 存储所有region节点的信息
    static Map<String, RegionThread> regionInfor = new HashMap<>();
    // 当前的master信息
    static RegionThread nowMaster = null;

    public static void main(String[] args) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Welcome to Distributed MiniSQL - ZooKeeperServer is running on port " + port + "\n");
            while (true) {
                // 监听端口
                Socket socket = serverSocket.accept(); // 等待客户端连接请求
                // IO请求
                BufferedReader in = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
                String request = in.readLine();
                if (request.startsWith("client")) {
                    // 分配给client线程处理
                    new Thread(new ClientThread(socket, out)).start();
                } else if (request.startsWith("region")) {
                    // 分配给region线程处理
                    new Thread(new RegionThread(socket, Integer.parseInt(request.split(":")[1]), "region")).start();
                } else if (request.startsWith("master")) {
                    // 分配给region线程处理
                    new Thread(new RegionThread(socket, Integer.parseInt(request.split(":")[1]), "master")).start();
                } else if (request.startsWith("server")) {
                    if (nowMaster == null) {
                        try {
                            out.write("master");
                            out.newLine();
                            out.flush();
                        } catch (IOException e) {
                            System.err.println("<ERROR>Failed to send message to the " + "server: " + e.getMessage());
                        }
                    } else {
                        try {
                            out.write("region");
                            out.newLine();
                            out.flush();
                        } catch (IOException e) {
                            System.err.println("<ERROR>Failed to send message to the " + "server: " + e.getMessage());
                        }
                    }
                    socket.close();
                } else {
                    System.out.println("<ERROR>invalid request");
                    socket.close();
                }
            }
        }
    }
}
