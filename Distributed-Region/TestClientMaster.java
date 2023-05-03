import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Scanner;

public class TestClientMaster {
    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";

        Scanner input = new Scanner(System.in);

        int myPort = input.nextInt();

        int port = 8081;
        // Connect to the server
        SocketAddress localaddr = new InetSocketAddress("127.0.0.1", myPort);
        SocketAddress endaddr = new InetSocketAddress("127.0.0.1", 8081);
        Socket socket = new Socket();
        socket.bind(localaddr);
        socket.connect(endaddr);
        System.out.println("Connected to server on " + host + ":" + port);

        // Send a message to the server
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        String line;
        try (Scanner sc = new Scanner(System.in)) {
            while ((line = sc.nextLine()) != null) {
                out.write(line);
                out.newLine();
                out.flush();
                if (line.contains(";")) {
                    StringBuilder sb = new StringBuilder(); // 用于存储所有读入的数据
                    char[] buf = new char[4096]; // 缓冲区大小可以根据需要调整
                    int len;
                    len = in.read(buf);
                    sb.append(buf, 0, len);
                    System.out.println(sb.toString());
                }
            }
        }
        socket.close();
    }
}