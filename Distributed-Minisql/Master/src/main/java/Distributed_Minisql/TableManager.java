package Distributed_Minisql;

import java.util.*;
import java.io.*;
public class TableManager {
    // 表格和存储它的主本Region IP的对应关系
    public Map<String, String> tableToIp;
    // 表格和存储它的副本Region IP的对应关系
    public Map<String, String> tableToCopyIp;
    // IP和处理这个IP的socket线程类的对应关系
    public Map<String, SocketThread> ipToSocket;
    // 一个记录所有连接过的Region Server IP的列表
    public List<String> serverList;
    // 记录从节点IP与它所存储的表的对应关系
    public Map<String, List<String>> ipToTables;

    //构造函数，为每张表分配内存
    public TableManager(){
        tableToIp = new HashMap<>();
        ipToSocket = new HashMap<>();
        serverList = new ArrayList<>();
        ipToTables = new HashMap<>();
        tableToCopyIp = new HashMap<>();
    }

    // 添加一张表
    // 输入：表名，主region服务器ip地址，副本region服务器ip地址
    public void addTable(String tableName, String ipAddress, String ipAddressCopy){
        tableToIp.put(tableName, ipAddress);
        tableToCopyIp.put(tableName, ipAddressCopy);
        // 更新IP和TABLE的关系，先判断该字典中是否有这个ipaddress的信息
        if (ipToTables.containsKey(ipAddress)) {
            ipToTables.get(ipAddress).add(tableName);
        }
        else {
            List<String> t = new ArrayList<>();
            t.add(tableName);
            ipToTables.put(ipAddress, t);
        }
        // 更新IP和TABLE的关系，只不过对于存储该表的副本服务器重新做了一次
        if (ipToTables.containsKey(ipAddressCopy)) {
            ipToTables.get(ipAddressCopy).add(tableName);
        }
        else {
            List<String> t = new ArrayList<>();
            t.add(tableName);
            ipToTables.put(ipAddressCopy, t);
        }
    }
    // 删除一张表
    public void deleteTable(String tableName, String ipAddress, String ipAddressCopy){
        // 从主本/副本列表中删除这个表名对应的记录，并从
        tableToIp.remove(tableName);
        tableToCopyIp.remove(tableName);
        ipToTables.get(ipAddress).removeIf(tableName::equals);
        ipToTables.get(ipAddressCopy).removeIf(tableName::equals);
    }

    // 查找某个tablename对应的主本服务器的IP
    public String getIpAddressMain(String tableName){
        for (Map.Entry<String, String> entry : tableToIp.entrySet()){
            if (entry.getKey().equals(tableName)){
                return entry.getValue();
            }
        }
        return null;
    }

    // 查找某个tablename对应的副本服务器的IP
    public String getIpAddressCopy(String tableName){
        for (Map.Entry<String, String> entry : tableToCopyIp.entrySet()){
            if (entry.getKey().equals(tableName)){
                return entry.getValue();
            }
        }
        return null;
    }

    // 出于负载均衡考虑，寻找当前负载最小的服务器存储新的表格
    // 该无参数方法选择存储了最少表格的服务器IP
    public String getBestServer(){
        Integer com = Integer.MAX_VALUE;
        String ret = "";
        for (Map.Entry<String, List<String>> entry : ipToTables.entrySet()){
            if (entry.getValue().size() < com){
                com = entry.getValue().size();
                ret = entry.getKey();
                System.out.println(entry.getKey()+"+");
            }
            // System.out.println(ret);
        }
        return ret;
    }

    // 参数：主机IP
    // 该有参数方法选择存储了最少表格的服务器IP，但把给定的hostIp排除在外
    public String getBestServer(String hostIp){
        Integer com = Integer.MAX_VALUE;
        String ret = "";
        for (Map.Entry<String, List<String>> entry : ipToTables.entrySet()){
            if (!entry.getKey().equals(hostIp) && entry.getValue().size() < com){
                com = entry.getValue().size();
                ret = entry.getKey();
            }
            // System.out.println(ret);
        }
        return ret;
    }

    // 参数：要添加的服务器IP字符串
    // 添加一台连接的服务器
    public void addServer(String IP){
        if (!existServer(IP)){
            serverList.add(IP);
        }
        // 为ipToTables映射添加一个空的表列表
        List<String> t = new ArrayList<>();
        ipToTables.put(IP, t);
    }

    // 给出服务器IP地址，返回是否已经和这个IP连接
    public boolean existServer(String IP){
        for (String s : serverList){
            if (s.equals(IP))
                return true;
        }
        return false;
    }

    // 参数：socketthread线程类对象，连接的端的IP地址
    // 添加一个socket线程
    public void addSocket(String IP, SocketThread socketThread){
        ipToSocket.put(IP, socketThread);
    }

    // 参数：IP地址
    // 给出这个IP对应的socket线程类，如果没有这个记录，返回NULL
    public SocketThread getSocketThread(String IP){
        for (Map.Entry<String, SocketThread> entry : ipToSocket.entrySet()){
            if (entry.getKey().equals(IP)){
                return entry.getValue();
            }
        }
        return null;
    }

    // 仅仅给ipToTables添加一条空的记录，用于一个连接过的regionserver重新连接时
    public void recoverServer(String IP){
        List<String> temp = new ArrayList<>();
        ipToTables.put(IP, temp);
    }
}

