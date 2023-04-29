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
            String str = input.readLine();
            process(str);
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
        if(cmds.length >= 0 && cmds[0].equals("Successfully")){
            if(cmds.length >= 3 && cmds[1].equals("create")){
                table.createSuccess(cmds[2], cmds[3]);
            }else if(cmds.length >= 3 && cmds[1].equals("drop")){
                table.dropSuccess(cmds[2], cmds[3]);
            }
            return;
        }else if(cmds.length >= 0 && cmds[0].equals("Create")){
            try {
                output.write(table.createRequest());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }else{
            boolean flag = false;
            for(String str : cmds){
                if(str.equalsIgnoreCase("where")){
                    flag = true;
                }else if(flag){
                    try {
                        output.write(table.normalRequest(str));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return;
                }
            }
        }
        try {
            output.write("Cann't find the table");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
