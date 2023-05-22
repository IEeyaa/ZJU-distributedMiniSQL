import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import Components.Region;
import src.components.Master;

public class RegionAll {
    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = "10.181.238.85";
        int port = 12345;
        int listenPort = 8081;
        Socket socket = null;
        BufferedReader input = null;
        BufferedWriter output = null;
        String msg = "";
        System.out.println("start distributed minisql server!");
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
            System.out.println(msg);
        } catch (IOException e) {
            System.out.println(e);
        }

        if (msg.equals("master")) {
            new Master(8086).start();
        } else {
            new Thread(new Region(ip, port, listenPort)).start();
        }
    }
}
