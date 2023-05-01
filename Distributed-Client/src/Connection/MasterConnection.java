package Connection;

import java.io.IOException;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class MasterConnection {
    private String ip;
    private int port;
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public MasterConnection(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String IP() {
        return ip;
    }

    public int Port() {
        return port;
    }

    public String toString() {
        return ip + ":" + port;
    }

    public boolean connect() {
        try {
            socket = new Socket(ip, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            return true;
        } catch (IOException e) {
            System.err.println("Failed to connect to the master server: " + e.getMessage());
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
            System.err.println("Failed to send message to the master server: " + e.getMessage());
            return false;
        }
    }

    public String receive() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            System.err.println("Failed to receive message from the master server: " + e.getMessage());
            return null;
        }
    }

    public boolean close() {
        try {
            socket.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to close the connection to the master server: " + e.getMessage());
            return false;
        }
    }

}
