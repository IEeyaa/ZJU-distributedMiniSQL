package Components;

import java.io.*;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;

import CATALOGMANAGER.Attribute;
import CATALOGMANAGER.CatalogManager;
import CATALOGMANAGER.Table;
import INDEXMANAGER.Index;
import INTERPRETER.Interpreter;

public class ClientThread implements Runnable {

    private BufferedReader in = null;
    private BufferedWriter out = null;

    private Socket socket;
    private int port;
    private String ip;
    private String type;

    // 结尾符
    static String endCode = "";

    public ClientThread(Socket socket) {
        this.socket = socket;
        this.port = socket.getPort();
        this.ip = socket.getInetAddress().getHostAddress();
        this.type = "client";
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
        int index;
        String line;
        StringBuilder statement = new StringBuilder();
        // Region 相关操作
        // 处理一个语句直至结束
        while (true) { // read whole statement until ';'
            line = receive();
            if (line == null) { // read the file tail
                in.close();
                return;
            } else if (line.contains("copy:")) {
                this.type = "region";
                System.out.println("A region has enter, his address is: " + ip + ":" + port);
                String table_name = line.split(":")[1];

                // 建立传输流
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

                Iterator<Map.Entry<String, Table>> iter = CatalogManager.tables.entrySet().iterator();
                // 传输catalogManeger信息
                dos.writeUTF("start_transform");
                Table tmpTable;
                while (iter.hasNext()) {
                    Map.Entry entry = iter.next();
                    tmpTable = (Table) entry.getValue();
                    if (tmpTable.tableName.equals(table_name)) {
                        // catalog读取传输
                        dos.writeUTF("table_catalog");
                        System.out.println("its put in");

                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        DataOutputStream tempDos = new DataOutputStream(byteArrayOutputStream);

                        // Write catalog information to the temporary output stream
                        tempDos.writeUTF(tmpTable.tableName);
                        tempDos.writeUTF(tmpTable.primaryKey);
                        tempDos.writeInt(tmpTable.rowNum);
                        tempDos.writeInt(tmpTable.indexNum);
                        for (int i = 0; i < tmpTable.indexNum; i++) {
                            Index tmpIndex = tmpTable.indexVector.get(i);
                            tempDos.writeUTF(tmpIndex.indexName);
                            tempDos.writeUTF(tmpIndex.attributeName);
                        }
                        tempDos.writeInt(tmpTable.attributeNum);
                        for (int i = 0; i < tmpTable.attributeNum; i++) {
                            Attribute tmpAttribute = tmpTable.attributeVector.get(i);
                            tempDos.writeUTF(tmpAttribute.attributeName);
                            tempDos.writeUTF(tmpAttribute.type.get_type().name());
                            tempDos.writeInt(tmpAttribute.type.get_length());
                            tempDos.writeBoolean(tmpAttribute.isUnique);
                        }

                        // Get the byte array from the temporary output stream
                        byte[] catalogBytes = byteArrayOutputStream.toByteArray();

                        // Send the byte array using dos.write
                        dos.writeInt(catalogBytes.length);
                        System.out.println(catalogBytes.length);
                        dos.write(catalogBytes);

                        break;
                    }
                }
                // 打开文件
                File file = new File(table_name);
                dos.writeUTF(table_name);
                // 发送数据流
                try (FileInputStream fis = new FileInputStream(file)) {
                    // Get the length of the file
                    int fileLength = (int) file.length();

                    // Write the length before writing the content
                    dos.writeInt(fileLength);

                    // Read the file content and write it to the output stream
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        dos.write(buffer, 0, bytesRead);
                    }
                    dos.flush();
                }
                dos.writeUTF("FILEEOF");
                close();
                break;
            } else if (line.contains(";")) { // last line
                index = line.indexOf(";");
                statement.append(line.substring(0, index));
                break;
            } else {
                statement.append(line);
                statement.append(" ");
            }
        }
        if (this.type == "client") {
            // after get the whole statement
            String main_sentence = statement.toString().trim().replaceAll("\\s+", " ");
            // to minisql
            System.out.println("A client has enter, his address is: " + ip + ":" + port);
            String result = Interpreter.interpret(main_sentence);
            send(result + endCode);
            if (!(result.startsWith("Syntax error") || result.startsWith("Run time error"))) {
                if (main_sentence.contains("create") || main_sentence.contains("insert")
                        || main_sentence.contains("drop") || main_sentence.contains("delete")) {
                    Region.masterThread.master_connector.send("(MODIFY)" + main_sentence + ";");
                }
            }
            close();

        }
    }

    public boolean send(String message) {
        try {
            System.out.println(message);
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