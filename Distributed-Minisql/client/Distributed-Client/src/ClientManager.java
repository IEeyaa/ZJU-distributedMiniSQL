
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Scanner;

public class ClientManager {
    MasterConnector masterconnector=null;
    CacheManager cachemanager=null;
    RegionConnector regionconnector=null;
    boolean flag=true;
    public ClientManager(){
        cachemanager=new CacheManager();
        // cachemanager.AddCache("table", "127.0.0.5 4430");
       
    }
    public String GetTableName(String sql){
        String [] tokens=sql.split("\\s+");
        if(tokens[0].equals("drop")||tokens[0].equals("insert")||tokens[0].equals("delete"))
            if(tokens[2].endsWith(";")) return tokens[2].substring(0,tokens[2].length()-1);
            else return tokens[2];

        if(tokens[0].equals("select")){
            for(int i=0;i<tokens.length;i++){
                if (tokens[i].equals("from") && i != tokens.length - 1) {
                    if(tokens[i+1].endsWith(";")) return tokens[i+1].substring(0,tokens[i+1].length()-1);
                    else return tokens[i+1];
                    // return tokens[i+1];
                }
            }
        }
        return "table";

    }
    public void run() throws UnknownHostException, IOException, InterruptedException{
        Scanner console=new Scanner(System.in);
        String allsql,str,sql;
     
        String ip;
        int port;
        masterconnector=new MasterConnector(cachemanager);
        do{
            System.out.println(">>> Please enter server's IP:");
            ip=console.next();
            if(ip.startsWith("quit;")){
                System.exit(0);    
            }
            System.out.println(">>> Please enter server's PORT:");
            port=console.nextInt();
           
        }while(masterconnector.ConenctToMaster(ip, port)==false);
        System.out.println(">>> Successfully connected to the master!!! You can use Distributed-MiniSQL now");
        regionconnector=new RegionConnector(cachemanager,masterconnector);
        while(flag){
            allsql="";
            str="";
            while(str.isEmpty()||str.endsWith(";")==false){
                
                str=console.nextLine();
                allsql=allsql+str+" ";
            }
            String []sqls= allsql.split(";");
            
            
            // System.out.println("输入的语句是"+allsql);
            for(int i=0;i<sqls.length-1;i++){
                sql=(sqls[i]+";").trim();
                
                if(sql.startsWith("quit;")){
                   
                    flag=false;
                    break;
                }
                else process(sql);
                
            }

        }
        
        masterconnector.release();
        console.close();
        return ;
    }
    public void process(String sql) throws InterruptedException{
        
        if(sql.startsWith("create")||sql.startsWith("drop")){//send to master directly
            masterconnector.send(sql);
        }
        
        else {
            String table = GetTableName(sql);
            //  table="table";///调试
            String regioncache=cachemanager.GetCache(table);
            if(regioncache!="null"&&regioncache!="unreachable"){
                if(regionconnector.ConnectToRegion(regioncache, sql)==false){
                    cachemanager.DelCache(table);
                }
                //错误可联通ip,recevie之后应该会删掉del，应该可以符合下面的null,啊 这个没做成阻塞的
            }
            Thread.sleep(100);
            if(cachemanager.GetCache(table).equals("null")){
                while(cachemanager.GetCache(table).equals("null")){
                    masterconnector.send("search:"+table);//向master发送表名，请求ip和port
                    Thread.sleep(100);
                }
                
                regioncache=cachemanager.GetCache(table);
                if(regioncache.equals("unreachable")){
                    cachemanager.DelCache(table);
                    System.out.println(">>> The table name you entered is incorrect. We cannot find it in the system!");
                }else{
                    regionconnector.ConnectToRegion(regioncache, sql);
                }
            }
            
        }

        return ;
    }
}
