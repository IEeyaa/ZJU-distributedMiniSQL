
import java.io.*;
import java.net.Socket;

// Client短连接
public class ClientThread implements Runnable {

    private BufferedWriter out = null;
    private Socket socket;
    private int port;
    private String ip;
    private String type;

    // 结尾符
    static String endCode = "";

    public ClientThread(Socket socket, BufferedWriter out) {
        this.socket = socket;
        this.port = socket.getPort();
        this.ip = socket.getInetAddress().getHostAddress();
        this.out = out;
    }

    public void run() {
        System.out.println(String.format("<client>Client %s:%d has enter", ip, port));
        if (ZooKeeper.nowMaster != null) {
            send(ZooKeeper.nowMaster.getAddress());
        } else {
            send("No master available");
        }
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean send(String message) {
        try {
            out.write(message);
            out.newLine();
            out.flush();
            return true;
        } catch (IOException e) {
            System.err.println("<ERROR>Failed to send message to the " + type + "server: " + e.getMessage());
            return false;
        }
    }
}