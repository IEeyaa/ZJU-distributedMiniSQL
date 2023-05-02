package Distributed_Minisql;

import java.net.Socket;
import java.io.*;

public class SocketT extends Thread {
    
    Socket socket;
    Table table;
    BufferedReader input = null;
    BufferedWriter output = null;

    /*
     * Function: a constructor with a Table object and a socket
     * Input:
     *  - socket: a Socket object
     *  - table: a Table object that needs to be record
     */
    public SocketT(Socket socket, Table table){
        this.socket = socket;
        this.table = table;
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
     * Function: continuely listen the command
     * Input: none
     * Output: none
     */
    @Override
    public void run(){
        try{
            while(true){
                String str = input.readLine();
                System.out.println("Read cmd from "+ socket.getInetAddress().getHostAddress() + ":" + str);
                process(str);
            }
        }catch(IOException e){
            System.out.println("Connection broken with "+ socket.getInetAddress().getHostAddress());
        }
    }

    /*
     * Function: Handle the command received
     * Input: - Cmd: a string which is the command
     * Output: none
     */
    private void process(String Cmd){
        String[] cmds = Cmd.split(" ");
        if(cmds.length >= 1 && cmds[0].equalsIgnoreCase("Successfully")){
            if(cmds.length >= 4 && cmds[1].equalsIgnoreCase("create")){
                table.createSuccess(cmds[2], cmds[3]);
            }else if(cmds.length >= 3 && cmds[1].equalsIgnoreCase("drop")){
                table.dropSuccess(cmds[2], cmds[3]);
            }
        }else if(cmds.length >= 1 && cmds[0].equalsIgnoreCase("Create")){
            try {
                output.write(table.createRequest());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            cmds = Cmd.split(":");
            if(cmds.length >= 2 && cmds[0].equals("search")){
                System.out.println("Reply: "+"<ip>:"+cmds[1]+":"+table.normalRequest(cmds[1]));
                try {
                    output.write("<ip>:"+cmds[1]+":"+table.normalRequest(cmds[1]));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            output.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
