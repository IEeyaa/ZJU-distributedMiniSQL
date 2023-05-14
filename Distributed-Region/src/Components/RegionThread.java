package Components;

import java.io.*;
import java.net.Socket;

import INTERPRETER.Interpreter;

public class RegionThread implements Runnable {

    private BufferedReader in = null;
    private BufferedWriter out = null;

    private Socket socket;
    private int port;
    private String ip;
    private String type;

    // 结尾符
    static String endCode = "";

    public RegionThread(Socket socket) {
        this.socket = socket;
        this.port = socket.getPort();
        this.ip = socket.getInetAddress().getHostAddress();
    }

    public void run() {
        try {
            in = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
            out = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
            try {
                // 预处理
                preload();
            } catch (IOException e) {
                System.out.println("101 Run time error : IO exception occurs");
            } catch (Exception e) {
                System.out.println("Default error: " + e.getMessage());
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 所有的语句都需要用分号作为结尾。
    public void preload() throws IOException {
        String restState = ""; // rest statement after ';' in last line
        int index;
        String line;
        StringBuilder statement = new StringBuilder();
        // Region 相关操作
        if (restState.contains("copy:")) {
            this.type = "region";
            System.out.println("A region has enter, his address is: " + ip + ":" + port);
            String table_name = restState.split(":")[1];
            try {
                // 打开文件
                FileReader fileReader = new FileReader(table_name);
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                // 发送文件内容
                String file_data = "";
                String temp = "";
                while ((temp = bufferedReader.readLine()) != null) {
                    file_data += temp;
                }
                // 关闭流
                bufferedReader.close();
                fileReader.close();
                send(file_data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // Client 相关操作
        else {
            System.out.println("A client has enter, his address is: " + ip + ":" + port);
            this.type = "client";
        }
        // 如果该语句就是一个分号
        if (restState.contains(";")) { // resetLine contains whole statement
            index = restState.indexOf(";");
            statement.append(restState.substring(0, index));
            restState = restState.substring(index + 1);
        } else {
            statement.append(restState); // add rest line
            statement.append(" ");
            // 处理一个语句直至结束
            while (true) { // read whole statement until ';'
                line = receive();
                if (line == null) { // read the file tail
                    in.close();
                    return;
                } else if (line.contains(";")) { // last line
                    index = line.indexOf(";");
                    statement.append(line.substring(0, index));
                    restState = line.substring(index + 1); // set reset statement
                    break;
                } else {
                    statement.append(line);
                    statement.append(" ");
                }
            }
        }
        // after get the whole statement
        String main_sentence = statement.toString().trim().replaceAll("\\s+", " ");
        // to minisql
        String result = Interpreter.interpret(main_sentence);
        send(result + endCode);
    }

    public void sendTableChange(String method, String tableName) {
        System.out.println("OVER");
        send("TABLECHANGE:" + Region.regionPort + ":" + method + ":" + tableName + endCode);
    }

    public boolean send(String message) {
        try {
            out.write(message);
            System.out.println(message);
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
            return null;
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