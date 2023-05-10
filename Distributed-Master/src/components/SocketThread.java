package components;

import java.net.Socket;
import java.io.*;

public class SocketThread extends Thread {
    
    Socket socket;
    String ip;
    Table table;
    BufferedReader input = null;
    BufferedWriter output = null;
    boolean running = true;
    long lasttime;

    /*
     * Function: a constructor with a Table object and a socket
     * Input:
     *  - socket: a Socket object
     *  - table: a Table object that needs to be record
     */
    public SocketThread(Socket socket, Table table){
        this.socket = socket;
        this.table = table;
        ip = socket.getInetAddress().getHostAddress() + ":";
        try{
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

    /*
     * Function: to send a message using socket
     * Input: - msg: the message to be sent
     * Output: none
     */
    private void send(String msg){
        try{
            output.write(msg);
            output.newLine();
            output.flush();
            System.out.println("Reply to " + ip + msg);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    /*
     * Function: continuely listen the command
     * Input: none
     * Output: none
     */
    @Override
    public void run(){
        try{
            while(running){
                String str = input.readLine();
                if(str == null) continue;
                System.out.println("Read cmd from "+ ip + ":" + str);
                process(str);
            }
            socket.close();
        }catch(IOException e){
            System.out.println("Connection broken with "+ ip);
        }
    }

    /*
     * Function: Handle the command received
     * Input: - Cmd: a string which is the command
     * Output: none
     */
    private void process(String Cmd){
        if(Cmd.startsWith("<")){
            if(Cmd.equalsIgnoreCase("<quit>")) {
                System.out.println("Socket with " + ip + "ends.");
                running = false;
            }else if(Cmd.equalsIgnoreCase("<create>")){
                send(table.createRequest());
            }else if(Cmd.startsWith("<get>")){
                String[] cmds = Cmd.split(">");
                if(cmds.length >= 2)
                    send(table.normalRequest(cmds[1]));
            }else if(Cmd.equalsIgnoreCase("<show>")){
                send(table.getTables());
            }
        }else if(Cmd.startsWith("(")){
            if(Cmd.startsWith("(hello)")){
                ip += Cmd.split("\\)")[1];
                table.addRegion(ip);
                lasttime = System.currentTimeMillis();
                new HeartBeat(this).start();
            }else if(Cmd.startsWith("(CREATE)")){
                String[] cmds = Cmd.split("\\)");
                if(cmds.length >= 2)
                    table.createSuccess(cmds[1], ip);
            }else if(Cmd.startsWith("(DROP)")){
                String[] cmds = Cmd.split("\\)");
                if(cmds.length >= 2)
                    table.dropSuccess(cmds[1], ip);                
            }else if(Cmd.startsWith("(ALIVE)")){
                lasttime = System.currentTimeMillis();
            }
        }
    }

    public void check() {
        if(System.currentTimeMillis() - lasttime > 20000){
            table.removeRegion(ip);
            running = false;
        }
    }
}
