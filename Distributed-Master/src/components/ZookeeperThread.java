package components;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class ZookeeperThread extends Thread{

    String ip;
    int port;
    Socket socket;
    BufferedReader input = null;
    BufferedWriter output = null;

    ZookeeperThread(String ip, int port){
        this.ip = ip;
        this.port = port;
    }

    /*
     * Function: to send a message using socket
     * Input: - msg: the message to be sent
     * Output: none
     */
    public void send(String msg) {
        try {
            output.write(msg);
            output.newLine();
            output.flush();
            System.out.println("Send to zookeeper:" + msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(){
        try{
            socket = new Socket(ip, port);
            input = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream()));
            output = new BufferedWriter(
                    new OutputStreamWriter(
                        socket.getOutputStream()));
        }catch(IOException e){
            System.out.println(e);
        }
        send("region");
        System.out.println("Successfully connect to zookeeper");
    }
}
