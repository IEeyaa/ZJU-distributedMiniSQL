package Components;

import java.io.*;
import Connection.Connect;
import java.util.Timer;
import java.util.TimerTask;

import CATALOGMANAGER.CatalogManager;
import INTERPRETER.Interpreter;
import INTERPRETER.API;

public class MasterThread implements Runnable {

    public Connect master_connector;
    private static long alive_time = 10000L;
    private int port;
    private String ip;
    private int regionListenPort;

    // 结尾符
    static String endCode = "";

    public MasterThread(String ip, int port, int regionListenPort) {
        this.ip = ip;
        this.port = port;
        this.regionListenPort = regionListenPort;
    }

    public void run() {
        try {
            master_connector = new Connect(ip, port, "master");
            master_connector.connect();
            System.out.println("connect master OK");
            master_connector.send(String.format(("(hello)%d:%s"), regionListenPort, CatalogManager.show_table()));

            // 创建定时器
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    // 每隔10秒发送一次"alive"
                    master_connector.send("(ALIVE)");
                    try {
                        API.store();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("OK");
                }
            }, 0L, alive_time);

            // 接受请求、发送请求【create drop insert delete】
            while (true) {
                String result = master_connector.receive();
                if (result != null) {
                    if (result.equals("ERROR")) {
                        // 处理master死亡
                        System.out.println("master died");
                        return;
                    } else if (result.startsWith("(copy)")) {
                        String infor = result.substring(result.indexOf(")") + 1);
                        String[] addresses = infor.split(":");
                        System.out.println(addresses);
                        copy_from_table(addresses[0], Integer.parseInt(addresses[1]), addresses[2]);
                    } else if (result.startsWith("(sql)")) {
                        String infor = result.substring(5);
                        int index = infor.indexOf(";");
                        String main_sentence = infor.substring(0, index).trim().replaceAll("\\s+", " ");
                        // to minisql
                        System.out.println(main_sentence);
                        System.out.println(Interpreter.interpret(main_sentence));
                    }
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void copy_from_table(String ip, int port, String tableName) throws Exception {
        System.out.println(String.format("COPY START from %s:%d tablename is %s", ip, port, tableName));
        Connect region_connector = new Connect(ip, port, "region");
        region_connector.connect();
        // 发送copy请求
        region_connector.send("copy:" + tableName);
        // 收到回复
        DataInputStream dis = new DataInputStream(region_connector.socket.getInputStream());

        String get_infor = "";
        get_infor = dis.readUTF();
        if (get_infor.equals("start_transform")) {
            while ((tableName = dis.readUTF()) != null) {
                if (tableName.equals("FILEEOF")) {
                    // 接收到传输结束标记，退出循环
                    break;
                } else if (tableName.equals("table_catalog") || tableName.equals("index_catalog")) {
                    int file_length = dis.readInt();
                    // 追加到file中
                    try (FileOutputStream fos = new FileOutputStream(tableName, true)) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        long totalBytesRead = 0;
                        while (totalBytesRead < file_length && (bytesRead = dis.read(buffer, 0,
                                (int) Math.min(buffer.length, file_length - totalBytesRead))) != -1) {
                            fos.write(buffer, 0, bytesRead);
                            totalBytesRead += bytesRead;
                        }
                        fos.close();
                    }
                } else {
                    // 重写
                    int file_length = dis.readInt();
                    try (FileOutputStream fos = new FileOutputStream(tableName, false)) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        long totalBytesRead = 0;
                        while (totalBytesRead < file_length && (bytesRead = dis.read(buffer, 0,
                                (int) Math.min(buffer.length, file_length - totalBytesRead))) != -1) {
                            fos.write(buffer, 0, bytesRead);
                            totalBytesRead += bytesRead;
                        }
                        fos.close();
                    }
                }
            }
        }
        System.out.println("COPY OVER");
        region_connector.close();
        // 热更新
        API.initial();
    }
}