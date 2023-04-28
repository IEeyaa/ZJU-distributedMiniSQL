import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;





public class Client {
    static Socket client;
    static DataOutputStream dos;
    static Receive receive;
    
    static String id;
    static String str;
    static boolean flag=true;
    
    static class Receive implements Runnable{
        private DataInputStream dis;
        public Receive(Socket client){
            try {
                dis=new DataInputStream(client.getInputStream());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                flag=false;
            }
            
        }
        private void getMessage(){
            String str=" ";
            try {
                str=dis.readUTF();
                System.out.println(str);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                flag=false;
            }
            
            
        }
        @Override
        public void run() {
            // TODO Auto-generated method stub
            while(flag){
                this.getMessage();
                
            }
        }
        
    }
    public Client(){
        try {
            client=new Socket("localhost",9999);
            dos=new DataOutputStream(client.getOutputStream());
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            flag=false;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            flag=false;
        }
        
        receive=new Receive(client);
        new Thread(receive).start();
  
    }
    
    // public static void main(String[] args) {
        
    //     try {
    //         client=new Socket("localhost",9999);
    //         dos=new DataOutputStream(client.getOutputStream());
    //     } catch (UnknownHostException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //         flag=false;
    //     } catch (IOException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //         flag=false;
    //     }
       
    //     receive=new Receive(client);
    //     new Thread(receive).start();
    //     Scanner console = new Scanner(System.in);
    //     while(true){
    //         String strs=console.nextLine();
    //         send(strs);
    //     }
        
    // }
    public  void send(String str){
        try {
            System.out.println(str);
            dos.writeUTF(str);
            dos.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          
        }
    }
}

