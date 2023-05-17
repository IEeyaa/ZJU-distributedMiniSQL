
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RegionThread implements Runnable {
    private static final long HEARTBEAT_TIMEOUT = 5000;
    private Socket socket;
    private int port;
    private String ip;
    private String type;
    private BufferedReader in = null;
    private BufferedWriter out = null;
    // 这里要建立一个数据结构来存储ip和port相关信息表;

    public RegionThread(Socket socket, int listenPort) {
        this.socket = socket;
        this.port = listenPort;
        this.ip = socket.getInetAddress().getHostAddress();
    }

    // 返回地址
    public String getAddress() {
        return String.format("%s:%d", this.ip, this.port);
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
        if (ZooKeeper.nowMaster == null) {
            ZooKeeper.nowMaster = this;
            this.type = "master";
            System.out.println("Region registered as master: " + getAddress());
            // Master会切换监听端口
            this.port = 8086;
            // 告知其为master
            send("master");
            try {
                connect_with_master();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            this.type = "region";
            // 告知其为region
            // 返回master地址和端口
            send(ZooKeeper.nowMaster.getAddress());
            ZooKeeper.regionInfor.put(getAddress(), this);
            connect_with_region();
        }
    }

    public void select_new_master() {
        // 选举出新的master
        Random random = new Random();
        List<String> keys = new ArrayList<String>(ZooKeeper.regionInfor.keySet());
        String randomKey = keys.get(random.nextInt(keys.size()));
        ZooKeeper.nowMaster = ZooKeeper.regionInfor.get(randomKey);
        ZooKeeper.nowMaster.send("toMaster");
        // 广播
        String master_address = ZooKeeper.nowMaster.getAddress();
        for (Map.Entry<String, RegionThread> entry : ZooKeeper.regionInfor.entrySet()) {
            String key = entry.getKey();
            RegionThread masterThread = entry.getValue();

            if (!key.equals(randomKey)) {
                // 跳过当前选出的 master
                masterThread.send("change:" + master_address);
            }
        }
    }

    public void remove_region(String address) throws IOException {
        if (ZooKeeper.regionInfor.containsKey(address)) {
            ZooKeeper.regionInfor.get(address).close();
            ZooKeeper.regionInfor.remove(address);
            System.out.println("remove successfully");
        } else {
            System.out.println("no region found");
        }
    }

    // 保持于master之间的连接所用
    public void connect_with_master() throws IOException {
        // 持续监听master发送的数据
        String result = "";
        long lastReceivedTime = System.currentTimeMillis(); // 记录最后一次接收到数据的时间戳
        while (true) {
            result = receive();
            // 连接意外中断
            if (result.equals("ERROR")) {
                System.out.println("master has closed socket");
                select_new_master();
                close();
                break;
            }
            // 收到心跳
            else if (result.equals("<ALIVE>")) {
                // 更新最后一次接收到数据的时间戳
                lastReceivedTime = System.currentTimeMillis();
            }
            // 移除Region
            else if (result.startsWith("remove")) {
                String removeIP = result.split("\\)")[1];
                remove_region(removeIP);
            }
            // 检查心跳超时
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastReceivedTime;
            if (elapsedTime > HEARTBEAT_TIMEOUT) {
                System.out.println("Heartbeat timeout");
                select_new_master();
                close();
                break;
            }
        }
        close();
    }

    // 保持于region之间的连接所用
    public void connect_with_region() {
        // 持续监听region发送的数据
        String result = "";
        while (true) {
            result = receive();
            if (result.equals("ERROR")) {
                // 处理连接中断
                System.out.println("region has closed socket");
            }
            close();
        }
    }

    // 辅助函数
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