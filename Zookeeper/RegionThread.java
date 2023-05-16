
import java.io.*;
import java.net.Socket;

public class RegionThread implements Runnable {
    private Socket socket;
    private int port;
    private String ip;
    private String type;
    private BufferedReader in = null;
    private BufferedWriter out = null;
    // 这里要建立一个数据结构来存储ip和port相关信息表;

    public RegionThread(Socket socket) {
        this.socket = socket;
        this.port = socket.getPort();
        this.ip = socket.getInetAddress().getHostAddress();
    }

    // 线程主函数
    public void run() {
        try {
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("A region has enter, its address is %s:%d", ip, port));
        // 选举Master
        if (ZooKeeper.masterIp == null || ZooKeeper.masterPort < 0) {
            ZooKeeper.masterIp = socket.getInetAddress().getHostAddress();
            ZooKeeper.masterPort = 8086;
            this.type = "master";
            System.out.println("Region registered as master: " + ZooKeeper.masterIp + ":" + ZooKeeper.masterPort);
            // 告知其为master
            send("master");
            connect_with_master();
        } else {
            this.type = "region";
            // 告知其为region
            // 返回master地址和端口
            send(ZooKeeper.masterIp + ":" + ZooKeeper.masterPort);
            connect_with_region();
        }
    }

    // 保持于master之间的连接所用
    public void connect_with_master() {
        // 持续监听master发送的数据
        String result = "";
        while (true) {
            result = receive();
            if (result.equals("ERROR")) {
                // 处理连接中断
                System.out.println("master has closed socket");
                close();
                break;
            } else if (result != null) {
                System.out.println(result);
                // do something
            }
            close();
        }
    }

    // 保持于region之间的连接所用
    public void connect_with_region() {
        // 持续监听region发送的数据
        String result = "";
        result = receive();
        if (result.equals("ERROR")) {
            // 处理连接中断
            System.out.println("region has closed socket");
        } else if (result != null) {
            System.out.println(result);
            // do something
        }
        close();
    }

    public boolean send(String message) {
        try {
            out.write(message);
            out.newLine();
            out.flush();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to send message to the " + type + "server: " + e.getMessage());
            return false;
        }
    }

    public String receive() {
        try {
            return in.readLine();
        } catch (IOException e) {
            System.err.println("Failed to receive message from the " + type + "server: " + e.getMessage());
            return "ERROR";
        }
    }

    public boolean close() {
        try {
            socket.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to close the connection to the " + type + "server: " + e.getMessage());
            return false;
        }
    }
}