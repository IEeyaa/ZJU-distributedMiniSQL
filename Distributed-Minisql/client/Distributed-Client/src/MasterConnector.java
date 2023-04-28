import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;






public class MasterConnector {
    private Socket socket=null;
    private BufferedReader cin;
    private BufferedWriter cout;
    // private DataOutputStream cout;
    private boolean isRunning=true;
    private String Host="localhost";
    private int Port=9999;
    private CacheManager cachemanager;
   
    
    public MasterConnector(CacheManager cache){
        
        cachemanager=cache;
    }
    public boolean  ConenctToMaster(String host,int port) throws IOException {
        
        try {
            socket=new Socket(host,port);
        } catch (SocketException e) {
            System.out.println(">>> The IP and PORT you entered cannot be connected. Please re-enter!! ");
            return false;
           
        }
        
        cout = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
        cin = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
        isRunning=true;
       
        new Thread(new Receive()).start();
        return true;
    }
    public void send(String str){
        try {
            cout.write(str);
            cout.newLine();
            cout.write("end");
            cout.newLine();
            cout.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        };
        
    }
    public void release() {
        isRunning = false;
        try {
            socket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    class Receive implements Runnable {
        
        public Receive() {
            
        
        }
        //接收消息
        private void receive() {
            String msg ="";
           
            try {
                while( (msg = cin.readLine()) != null) {
                    if (msg.equals("end")) {
                        // release();
                        // break;
                        continue;
                    }
                    if(msg.startsWith("<ip>:")){// 
                        String [] ips=msg.split(":");
                        cachemanager.AddCache(ips[1], ips[2]);
                       
                    }
                    
                    else System.out.println(">>> "+msg);
                }
               
            } catch (IOException e) {
                
                
                 release();
                
            }
           
        }
        
        @Override
        public void run() {		
            while(isRunning) {
                receive();
            }
        }
        //释放资源
        
    }
}

