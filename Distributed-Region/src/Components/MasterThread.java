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
                    }
                    String method = result.substring(result.indexOf("(") + 1, result.indexOf(")"));
                    String infor = result.substring(result.indexOf(")") + 1);
                    switch (method) {
                        case "copy":
                            String[] addresses = infor.split(":");
                            copy_from_table(addresses[0], Integer.parseInt(addresses[1]), addresses[2]);
                            break;
                        case "sql":
                            String main_sentence = infor.trim().replaceAll("\\s+", " ");
                            // to minisql
                            Interpreter.interpret(main_sentence);
                            break;
                        default:
                            break;
                    }
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void copy_from_table(String ip, int port, String tableName) throws Exception {
        System.out.println(String.format("copy from %s:%d tablename is %s", ip, port, tableName));
        Connect region_connector = new Connect(ip, port, "region");
        region_connector.connect();
        // 发送copy请求
        region_connector.send("copy:" + tableName);
        System.out.println("has send ");
        // 收到回复
        while (true) {
            String result = region_connector.receive();
            if (result.equals("FILEEOF")) {
                break;
            }
            String[] file_infor = result.split("\\$");
            // 检查文件是否存在，如果存在，则清除文件内容
            File file = new File(file_infor[0]);
            if (file.exists()) {
                try (PrintWriter writer = new PrintWriter(file)) {
                    writer.print("");
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 创建一个新文件
            try (FileWriter writer = new FileWriter(file)) {
                writer.write(file_infor[1]);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("COPY OVER");
        region_connector.close();
        // 热更新
        API.initial();
    }
}