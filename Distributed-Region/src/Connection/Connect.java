package Connection;

import java.io.IOException;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Connect {
    private String type;
    private String ip;
    private int port;

    public Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Connect(String ip, int port, String type) {
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
            System.err.println("<ERROR>Failed to connect to the " + type + "server: " + e.getMessage());
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
            System.err.println("<ERROR>Failed to send message to the " + type + "server: " + e.getMessage());
            return false;
        }
    }

    public String receive() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            System.err.println("<ERROR>Failed to receive message from the " + type + "server: " + e.getMessage());
            return "ERROR";
        }
    }

    public boolean close() {
        try {
            socket.close();
            return true;
        } catch (IOException e) {
            System.err.println("<ERROR>Failed to close the connection to the " + type + "server: " + e.getMessage());
            return false;
        }
    }

}