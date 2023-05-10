package Connection;

import java.io.IOException;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Connection {
    private String type;
    private String ip;
    private int port;

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Connection(String ip, int port, String type) {
        this.type = type;
        this.ip = ip;
        this.port = port;
    }

    public String Type() {
        return type;
    }

    public String IP() {
        return ip;
    }

    public int Port() {
        return port;
    }

    public String toString() {
        return type + " " + ip + ":" + port;
    }

    public boolean connect() {
        try {
            socket = new Socket(ip, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            return true;
        } catch (IOException e) {
            System.err.println("Failed to connect to the " + type + "server: " + e.getMessage());
            return false;
        }
    }

    public boolean send(String message) {
        try {
            writer.write(message);
            writer.newLine();
            writer.flush();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to send message to the " + type + "server: " + e.getMessage());
            return false;
        }
    }

    public String receive() {
        try {
            String res = reader.readLine();
            return res.replaceAll("\\$", "\n").trim();
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
