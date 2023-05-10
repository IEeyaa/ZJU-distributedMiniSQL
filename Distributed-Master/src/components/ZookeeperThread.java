package components;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class ZookeeperThread extends Thread{

    Socket socket;
    BufferedReader input = null;
    BufferedWriter output = null;

    ZookeeperThread(String ip, int port){
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
    }

    @Override
    public void run(){
        try {
            output.write("region");
            output.newLine();
            output.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Successfully connect to zookeeper");
    }
}
