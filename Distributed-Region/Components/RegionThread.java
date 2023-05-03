package Components;

import java.io.*;
import java.net.Socket;

import INTERPRETER.Interpreter;

public class RegionThread implements Runnable {

    private BufferedReader in = null;
    private BufferedWriter out = null;

    private Socket socket;
    private int port;
    private String type;

    // 结尾符
    static String endCode = "$end";

    public RegionThread(Socket socket, String type) {
        this.socket = socket;
        this.port = socket.getPort();
        this.type = type;
        if (type.equals("master")) {
            Region.MasterThread = this;
            System.out.println(123);
        }
    }

    public void run() {
        try {
            System.out.println("A" + type + " has enter, his port is: " + port);
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
        while (true) { // read for each statement
            int index;
            String line;
            StringBuilder statement = new StringBuilder();
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
            String method = main_sentence.split(":")[0];
            switch (method) {
                case "execute":
                    String sql_sentence = main_sentence.split(":")[1];
                    String result = Interpreter.interpret(sql_sentence);
                    send(result + endCode);
                    break;
                case "detect":
                    out.write("alive!");
                    out.newLine();
                    out.flush();
                    break;
                case "get_table":
                    result = Interpreter.interpret("show tables");
                    send(result + endCode);
                    break;
                case "quit":
                    send("bye" + endCode);
                    break;
                default:
                    send("UNKNOWN" + endCode);
                    break;
            }
        }
    }

    public void sendTableChange(String method, String tableName) {
        System.out.println("OVER");
        send("TABLECHANGE:" + Region.regionPort + ":" + method + ":" + tableName + endCode);
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