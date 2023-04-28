
import java.util.HashMap;
// import java.util.Scanner;

public class CacheManager {
    private static HashMap<String,RegionInfo> TableRegionList=new HashMap<>();
    private static int LiveTime=3600000;//ms  equals one hour
    private static int MaxCacheSize=1000;//max cache number

    public CacheManager(){

    }

    public  boolean AddCache(String table,RegionInfo region){
        if(TableRegionList.size()>MaxCacheSize){
            ClearOldData();
        }
        TableRegionList.put(table,region);
        return true;
    }
    public  boolean AddCache(String table,String region){//"ip+" "+port" 
        if(TableRegionList.size()>MaxCacheSize){
            ClearOldData();
        }
        RegionInfo info;
        if(region.equals("unreachable")){
            info=new RegionInfo("0", 0,false);
        }
        else{
            String[] infostrs=region.split(" ");
            // System.out.println("add cache:"+infostrs[0]);
            info=new RegionInfo(infostrs[0], 8080, true);//暂定8080默认端口
            // info=new RegionInfo(infostrs[0], Integer.parseInt(infostrs[1]), true);
        }
        
        TableRegionList.put(table,info);
        return true;
    }
    public  boolean DelCache(String table){
        TableRegionList.remove(table);
        return true;
    }
    public String GetCache(String table){
        RegionInfo tmp= TableRegionList.get(table);
        if(tmp==null) return "null";
        if(tmp.savetime<0) return "unreachable";

        return tmp.ip+" "+tmp.port;
    }
    public  void ClearOldData(){
        long nowtime=System.currentTimeMillis();
        for(String key:TableRegionList.keySet()){
            RegionInfo tmp =TableRegionList.get(key);
            if(nowtime-tmp.savetime>LiveTime){
                TableRegionList.remove(key);
            }
        }
    }
}
class RegionInfo{
    String ip;
    int port;
    long savetime;//-1 means unreachable
    RegionInfo(String ip,int port,boolean reach){
        this.ip=ip;
        this.port=port;
        if(reach==false)
            this.savetime=-1;
        else
            this.savetime=System.currentTimeMillis();//total ms from 1970
    }
    

   
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "ip= "+this.ip+"; port= "+this.port+"\n";
    }
}
