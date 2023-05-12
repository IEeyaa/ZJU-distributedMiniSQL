package components;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;

public class Table {

    ZookeeperThread zookeeper;
    // table -> main region's ip and port
    private HashMap<String, String> tableToMainIp = new HashMap<>();
    // table -> slave region's ip and port
    private HashMap<String, String> tableToSlaveIp = new HashMap<>();
    // ip and port -> socket
    private HashMap<String, SocketThread> ipToSocket = new HashMap<>();
    // ip and port -> tables
    private HashMap<String, ArrayList<String>> ipToTables = new HashMap<>();
    // a list of all region servers' ip and port
    // private ArrayList<String> regions = new ArrayList<>();

    Table(ZookeeperThread zookeeper) {
        this.zookeeper = zookeeper;
    }

    /*
     * Function: Select a region server to handle the create table request
     * Input: none
     * Ouput: a string of the selected region server's ip and port
     */
    public String createRequest() {
        int min = Integer.MAX_VALUE;
        String ip = null;
        for (String i : ipToTables.keySet()) {
            if (ipToTables.get(i).size() < min) {
                min = ipToTables.get(i).size();
                ip = i;
            }
        }
        // System.out.println("select "+ ip +" to create");
        return ip;
    }

    /*
     * Funtion: Find the region server who shall handle the request
     * Input: - tableName: a string of the table's name, which comes from the
     * request
     * Output: a string of the region server's ip and port
     */
    public String normalRequest(String tableName) {
        String ip = tableToMainIp.get(tableName);
        if (ip != null) {
            // System.out.println("head "+ip+" to work");
            return ip;
        }
        return "unreachable";
    }

    /*
     * Function: Update metadata after the region server successfully handle the
     * create request
     * Input:
     * - tableName: a string of the table's name, which is newly created
     * - regionIp: a string of the region server's ip and port, who just
     * successfully handle the create request
     * Output: none
     */
    public void createSuccess(String tableName, String regionIp) {
        tableToMainIp.put(tableName, regionIp);
        if (ipToTables.containsKey(regionIp)) {
            ipToTables.get(regionIp).add(tableName);
        } else {
            ArrayList<String> a = new ArrayList<>();
            a.add(tableName);
            ipToTables.put(regionIp, a);
        }
        System.out.println("Successfully create");
    }

    /*
     * Function: Update metadata after the region server successfully handle the
     * drop request
     * Input:
     * - tableName: a string of the table's name, which is just droped
     * - regionIp: a string of the region server's ip and port, who just
     * successfully handle the drop request
     * Output: none
     */
    public void dropSuccess(String tableName, String regionIp) {
        tableToMainIp.remove(tableName, regionIp);
        ipToTables.get(regionIp).remove(tableName);
        System.out.println("Successfully drop");
    }

    /*
     * Function: Record the relationship between the ip and socket
     * Input:
     * - ip: a string of ip and port
     * - socket: a SocketThread object
     * Output: none
     */
    public void addSocket(String ip, SocketThread socket) {
        ipToSocket.put(ip, socket);
    }

    /*
     * Function: Add a region into record
     * Input: - ip: a string of region's ip and port
     * Output: none
     */
    public void addRegion(String ip) {
        // regions.add(ip);
        ipToTables.put(ip, new ArrayList<String>());
        System.out.println("Add a new region:" + ip);
    }

    /*
     * Function: Remove a region from record and recover
     * Input: - ip: a string of region server's ip and port
     * Output: none
     */
    public void removeRegion(String ip) {
        for (String i : ipToTables.get(ip)) {
            String anotherIP = selectExcept(ip);
            if (tableToMainIp.get(i).equals(ip)) {
                tableToMainIp.put(i, anotherIP);
                ipToSocket.get(anotherIP).send("(copy)" + tableToSlaveIp.get(i) + ":" + i);
            } else {
                tableToSlaveIp.put(i, anotherIP);
                ipToSocket.get(anotherIP).send("(copy)" + tableToMainIp.get(i) + ":" + i);
            }
        }
        // regions.remove(ip);
        zookeeper.send("(remove)" + ip);
        ipToTables.remove(ip);
        ipToSocket.remove(ip);
        System.out.println("Remove a region:" + ip);
    }

    /*
     * Function: get all tables in the database
     * Input: none
     * Output: a string of all tables
     */
    public String getTables() {
        String ans = "{$";
        for (String table : tableToMainIp.keySet()) {
            ans += table + "$";
        }
        ans += "}";
        return ans;
    }

    /*
     * Function: to find a region server with fewest tables except the main region
     * server
     * Input: - mainIP: a string of main region's ip and port
     * Output: a string of selected region server's ip and port
     */
    private String selectExcept(String mainIP) {
        int min = Integer.MAX_VALUE;
        String slaveIP = null;
        for (String i : ipToTables.keySet()) {
            if (!i.equals(mainIP) && ipToTables.get(i).size() < min) {
                min = ipToTables.get(i).size();
                slaveIP = i;
            }
        }
        return slaveIP;
    }

    /*
     * Function: to handle the modification on main region
     * Input:
     * - sql: a string of the sql statement which have done on main region
     * - mainIP: a string of main region's ip and port
     * Output: none
     */
    public void handleSQL(String sql, String mainIP) {
        if (getMethod(sql).equals("create")) {
            String slaveIP = selectExcept(mainIP);
            if (slaveIP != null)
                ipToSocket.get(slaveIP).send("(sql)" + sql);
        } else {
            String slaveIP = tableToSlaveIp.get(getTables(sql));
            if (slaveIP != null) {
                ipToSocket.get(slaveIP).send("(sql)" + sql);
            }
        }
    }

    /*
     * Function: to find the method from a sql statement
     * Input: - sql: a string of sql statement
     * Output: a string of method
     */
    private static String getMethod(String sql) {
        String[] parts = sql.split("\\s+");
        return parts.length > 0 ? parts[0].toLowerCase() : null;
    }

    /*
     * Function: to find the table name from a sql statement
     * Input: - sql: a string of sql statement
     * Output: a string of table name
     */
    private static String getTables(String sql) {
        ArrayList<String> tables = new ArrayList<>();
        Pattern pattern = Pattern.compile(
                "(?:FROM|JOIN|INTO|TABLE)\s+([^\s;\\(]+)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                tables.add(matcher.group(1));
            } else {
                tables.add(matcher.group(2));
            }
        }
        return tables.size() == 1 ? tables.get(0).toLowerCase() : null;
    }
}
