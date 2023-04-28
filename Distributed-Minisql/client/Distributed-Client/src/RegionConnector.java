import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.Iterator;

import org.json.JSONArray;


public class RegionConnector {
    
    private CacheManager cachemanager;
    private MasterConnector masterconnector;
    public RegionConnector(CacheManager cache,MasterConnector master){
        cachemanager=cache;
        masterconnector=master;
    }
    public boolean ConnectToRegion(String info,String sql){
        ToRegion toregion=new ToRegion(info,sql);
        if(toregion.ConnectThisRegion()==false) return false;
        new Thread(toregion).start();
        toregion.send(sql);
        return true;
    }
    
    


    class ToRegion implements Runnable {
       
        
        
        private Socket socket;
        // private DataInputStream cin;
        // private DataOutputStream cout;
        private BufferedReader cin;
        private BufferedWriter cout;
        private String ip;
        private int port;
        private boolean isRunning=true;
        private String sql;
        public ToRegion(String info,String sql) {
            String[] infos=info.split(" ");
            ip=infos[0];
            port=Integer.parseInt(infos[1]);
            this.sql=sql;
        }
        public boolean ConnectThisRegion(){
            try {
                socket=new Socket(ip,port);
                
            } catch (SocketException e) {
                // System.out.println("您要连接的region"+ip+" "+port+"有误");
                return false;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                cin = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                cout = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
                // cout=new DataOutputStream(socket.getOutputStream());
                // cin=new DataInputStream(socket.getInputStream());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return true;
        }
        public void send(String str){
            try {
               
                cout.write("execute:"+str);
                cout.newLine();
                cout.write("end");
                cout.newLine();
            
                cout.flush();
            } catch (IOException e) {
                e.printStackTrace();
            };
            
        }

        private void receive() {
            String msg ="";
           
            try {
                while( (msg = cin.readLine()) != null) {
                    if (msg.equals("end")) {
                        release();
                        break;
                    }
                   
                    if(msg.startsWith("[")){
                        System.out.println(">>> "+msg);
                        msg=msg.replace("=", ":");
                        System.out.print(">>> \n");
                        // 解析select 返回的json串
                        JSONArray jsonArray = new JSONArray(msg);
                        Iterator iterator1 = jsonArray.getJSONObject(0).keys();
                        System.out.print("\t|");
                        while(iterator1.hasNext()){
                            String key = (String)iterator1.next();
                            System.out.print(key+"|");
                        }
                         System.out.print("\n");
                        // System.out.print("--------------------\n");
                        for(int i=0;i<jsonArray.length();i++){
                            Iterator iterator = jsonArray.getJSONObject(i).keys();
                            while(iterator.hasNext()){
                                String key = (String)iterator.next();

                                System.out.print("\t|"+jsonArray.getJSONObject(i).get(key));
                            }
                            System.out.println("|");
                        }
                    } 
                    else if(msg.contains("Table")& msg.contains("not exist")){
                        String[] msgstr=msg.split(" ");
                        for(int i=0;i<msgstr.length;i++){
                            if(msgstr[i].equals("Table")){
                                cachemanager.DelCache(msgstr[i+1]);
                                break;
                            }
                        }


                    }
                    else    System.out.println(">>> "+msg);
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
            return ;
        }
        //释放资源
        public void release() {
            isRunning = false;
            try {
                socket.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // System.out.println("已断开和该region的连接");
        }
    }
}

