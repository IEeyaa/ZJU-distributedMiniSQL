package components;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Listener {
    
    Table table;

    /*
     * Function: a constructor with a Table object
     * Input: - table: a Table object that needs to be record
     */
    public Listener(Table table){
        this.table = table;
    }

    /*
     * Function: Start to listen on a specified port and start a thead whenever a new socket is created
     * Input: - port: an int on which the master will listen
     * Output: none
     */
    protected void startListen(int port) throws IOException{
        ServerSocket s = new ServerSocket(port);
        System.out.println("Start listen on " + port);
        try{
            while(true){
                Socket socket = s.accept();
                SocketThread thread = new SocketThread(socket, table);
                table.addSocket(socket.getInetAddress().getHostAddress(), thread);
                System.out.println("Establish a socket with " + socket.getInetAddress().getHostAddress());
                thread.start();
            }
        }finally{
            s.close();
        }
    }
}
