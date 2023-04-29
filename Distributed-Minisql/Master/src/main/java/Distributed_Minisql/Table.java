package Distributed_Minisql;

import java.util.HashMap;
import java.util.ArrayList;

public class Table {

    // table -> main region
    public HashMap<String, String> tableToMainIp = new HashMap<>();
    // table -> slave region
    // public HashMap<String, String> tableToSlaveIp = new HashMap<>();
    // ip -> socket
    public HashMap<String, SocketT> ipToSocket = new HashMap<>();
    // ip -> tables
    public HashMap<String, ArrayList<String>> ipToTables = new HashMap<>();
    // a list of all region servers' ip
    public ArrayList<String> regions = new ArrayList<>();
    
    /*
     * Function: Select a region server to handle the create table request
     * Input: none
     * Ouput: a string of the selected region server's ip
     */
    public String createRequest(){
        int min = Integer.MAX_VALUE;
        String ip = null;
        for(String i : ipToTables.keySet()){
            if(ipToTables.get(i).size() < min){
                min = ipToTables.get(i).size();
                ip = i;
            }
        }
        return ip;
    }

    /*
     * Funtion: Find the region server who shall handle the request
     * Input: - tableName: a string of the table's name, which comes from the request
     * Output: a string of the region server's ip
     */
    public String normalRequest(String tableName){
        return tableToMainIp.get(tableName);
    }

    /*
     * Function: Update metadata after the region server successfully handle the create request
     * Input:
     *  - tableName: a string of the table's name, which is newly created
     *  - regionIp: a string of the region server's ip, who just successfully handle the create request
     * Output: none
     */
    public void createSuccess(String tableName, String regionIp){
        tableToMainIp.put(tableName, regionIp);
        if(ipToTables.containsKey(regionIp)){
            ipToTables.get(regionIp).add(tableName);
        }else{
            ArrayList<String> a = new ArrayList<>();
            a.add(tableName);
            ipToTables.put(regionIp, a);
        }
    }

    /*
     * Function: Update metadata after the region server successfully handle the drop request
     * Input:
     *  - tableName: a string of the table's name, which is just droped
     *  - regionIp: a string of the region server's ip, who just successfully handle the drop request
     * Output: none
     */
    public void dropSuccess(String tableName, String regionIp){
        tableToMainIp.remove(tableName, regionIp);
        ipToTables.get(regionIp).remove(tableName);
    }

    /*
     * Function: Record the relationship between the ip and socket
     * Input:
     *  - ip: a string of ip
     *  - socket: a SocketThread object
     * Output: none
     */
    public void addSocket(String ip, SocketT socket){
        ipToSocket.put(ip, socket);
    }
}
