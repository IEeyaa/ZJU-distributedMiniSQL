package Distributed_Minisql;

import java.util.HashMap;
import java.util.ArrayList;

public class Table {

    // table -> main region
    private HashMap<String, String> tableToMainIp = new HashMap<>();
    // table -> slave region
    // private HashMap<String, String> tableToSlaveIp = new HashMap<>();
    // ip -> socket
    private HashMap<String, SocketT> ipToSocket = new HashMap<>();
    // ip -> tables
    private HashMap<String, ArrayList<String>> ipToTables = new HashMap<>();
    // a list of all region servers' ip
    private ArrayList<String> regions = new ArrayList<>();
    
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
        // System.out.println("select "+ ip +" to create");
        return ip;
    }

    /*
     * Funtion: Find the region server who shall handle the request
     * Input: - tableName: a string of the table's name, which comes from the request
     * Output: a string of the region server's ip
     */
    public String normalRequest(String tableName){
        String ip = tableToMainIp.get(tableName);
        if(ip != null){
            System.out.println("head "+ip+" to work");
            return tableToMainIp.get(tableName);
        }
        return "unreachable";
    }

    /*
     * Function: Update metadata after the region server successfully handle the create request
     * Input:
     *  - tableName: a string of the table's name, which is newly created
     *  - regionIp: a string of the region server's ip, who just successfully handle the create request
     * Output: none
     */
    public void createSuccess(String tableName, String regionIp){
        tableToMainIp.put(tableName, regionIp+":8080");                 //
        if(ipToTables.containsKey(regionIp)){
            ipToTables.get(regionIp).add(tableName);
        }else{
            ArrayList<String> a = new ArrayList<>();
            a.add(tableName);
            ipToTables.put(regionIp, a);
        }
        System.out.println("Successfully create");
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
        System.out.println("Successfully drop");
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

    /*
     * Function: Add a region into record
     * Input: - ip: a string of region's ip
     * Output: none
     */
    public void addRegion(String ip){
        regions.add(ip);
        ipToTables.put(ip, new ArrayList<String>());
    }
}
