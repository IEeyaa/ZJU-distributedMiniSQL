import components.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

import Components.Region;

public class RegionAll {
    public static void main(String[] args) throws IOException, InterruptedException {
        String ip = "10.162.90.213";
        int port = 12345;
        Socket socket = null;
        BufferedReader input = null;
        BufferedWriter output = null;
        String msg = "";

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

        if (msg.split(":")[0].equals(socket.getLocalAddress()) && msg.split(":").equals("8081")) {
            new Master().start();
        } else {
            new Thread(new Region("127.0.0.1", 12345, 8081)).start();
        }
    }
}
