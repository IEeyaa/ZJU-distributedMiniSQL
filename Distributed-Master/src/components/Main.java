package src.components;

import java.io.BufferedReader;
import java.io.*;
import java.net.Socket;

public class Main {
    public static void main(String args[]) {
        String ip = "192.168.43.76";
        int port = 12345;
        Socket socket;
        BufferedReader input = null;
        BufferedWriter output = null;
        String msg;

        try {
            socket = new Socket(ip, port);
            input = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream()));
            output = new BufferedWriter(
                    new OutputStreamWriter(
                            socket.getOutputStream()));
            output.write("server");
            output.newLine();
            output.flush();
            msg = input.readLine();
        } catch (IOException e) {
            System.out.println(e);
        }
        try {
            Master master = new Master();
            master.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
