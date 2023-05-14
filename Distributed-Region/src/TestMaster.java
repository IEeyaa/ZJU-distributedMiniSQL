import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import Connection.Connect;

public class TestMaster {
    public static void main(String[] args) throws Exception {
        int port = 8086;

        // Connect to the server
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // 每当出现新的连接，则建立一个线程来处理
            System.out.println("master running at port 8086");
            while (true) {
                Socket socket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                String line;
                try (Scanner sc = new Scanner(System.in)) {
                    while ((line = sc.nextLine()) != null) {
                        out.write(line);
                        out.newLine();
                        out.flush();
                    }
                }
            }
        }
    }
}