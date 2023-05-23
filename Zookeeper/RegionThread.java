
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RegionThread implements Runnable {
    private static final long HEARTBEAT_TIMEOUT = 12000;
    private Socket socket;
    private int port;
    private String ip;
    private String type;
    private BufferedReader in = null;
    private BufferedWriter out = null;
    // 这里要建立一个数据结构来存储ip和port相关信息表;

    public RegionThread(Socket socket, int listenPort, String type) {
        this.socket = socket;
        this.port = listenPort;
        this.type = type;
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
        // 选举Master
        if (this.type.equals("master")) {
            ZooKeeper.nowMaster = this;
            System.out.println(String.format("<master>Server %s registered as master", getAddress()));
            // 告知其为master
            try {
                connect_with_master();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            // 告知其为region
            // 返回master地址和端口
            System.out.println(String.format("<region>Server %s registered as region", getAddress()));
            send(ZooKeeper.nowMaster.getAddress());
            ZooKeeper.regionInfor.put(getAddress(), this);
        }
    }

    public void select_new_master() throws InterruptedException {
        // 选举出新的master
        Random random = new Random();
        List<String> keys = new ArrayList<String>(ZooKeeper.regionInfor.keySet());
        String new_master_address = keys.get(random.nextInt(keys.size()));

        // 只有一个Region
        if (keys.size() <= 1) {
            System.out.println("<master_change>no enough server! waiting for server to enter");
            ZooKeeper.nowMaster.close();
            ZooKeeper.regionInfor.remove(ZooKeeper.nowMaster.getAddress());
            ZooKeeper.nowMaster = null;
        }
        ZooKeeper.nowMaster.close();
        ZooKeeper.regionInfor.remove(ZooKeeper.nowMaster.getAddress());

        System.out.println(String.format("<master_change>master have changed to %s", new_master_address));
        ZooKeeper.regionInfor.get(new_master_address).send("toMaster");

        Thread.sleep(10L);
        // 广播
        String master_address = new_master_address;
        for (Map.Entry<String, RegionThread> entry : ZooKeeper.regionInfor.entrySet()) {
            String key = entry.getKey();
            RegionThread masterThread = entry.getValue();

            if (!key.equals(new_master_address)) {
                // 跳过当前选出的 master
                masterThread.send("change:" + master_address.split(":")[0] + ":8086");
            }
        }
    }

    public void remove_region(String address) throws IOException {
        if (ZooKeeper.regionInfor.containsKey(address)) {
            ZooKeeper.regionInfor.get(address).close();
            ZooKeeper.regionInfor.remove(address);
            System.out.println(String.format("<region_move>remove %s successfully", address));
        } else {
            System.out.println("<region_move>ERROR! no region found");
        }
    }

    // 保持于master之间的连接所用
    public void connect_with_master() throws IOException, InterruptedException {
        // 持续监听master发送的数据
        String result = "";
        long lastReceivedTime = System.currentTimeMillis(); // 记录最后一次接收到数据的时间戳
        while (true) {
            result = receive();
            // 连接意外中断
            if (result.equals("ERROR")) {
                System.out.println("<master>master died");
                select_new_master();
                break;
            }
            // 收到心跳
            else if (result.equals("ALIVE")) {
                // 更新最后一次接收到数据的时间戳
                lastReceivedTime = System.currentTimeMillis();
            }
            // 移除Region
            else if (result.startsWith("(remove)")) {
                String removeIP = result.split("\\)")[1];
                remove_region(removeIP);
            }
            // 检查心跳超时
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastReceivedTime;
            if (elapsedTime > HEARTBEAT_TIMEOUT) {
                System.out.println("<master>master has no heartbeat!");
                select_new_master();
                break;
            }
        }
        close();
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