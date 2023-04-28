import java.io.*;
import java.io.BufferedReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.plaf.synth.Region;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import CATALOGMANAGER.Attribute;
import CATALOGMANAGER.CatalogManager;
import CATALOGMANAGER.NumType;
import CATALOGMANAGER.Table;
import INDEXMANAGER.Index;
import INDEXMANAGER.IndexManager;
import RECORDMANAGER.Condition;
import RECORDMANAGER.RecordManager;
import RECORDMANAGER.TableRow;

public class Interpreter{

    static String masterIp = "";
    static Map <String, List<String>> regionMap = new HashMap<String, List<String>>();
    static String MasterIP = "127.0.0.1";
    static String ZookeeperIP = "127.0.0.1:2181";
    static Socket MasterSocket;
    public static void main(String[] args) throws Exception{
        System.out.println("hello");
        API.initial();
        //joinInZookeeper();
        ipadd();
        int port = 8080;
        ServerSocket serverSocket = new ServerSocket(port);
        while(true){
            Socket socket = serverSocket.accept();
            new Thread(new task(socket)).start();
        }
    }

    public static String ipadd() throws Exception {
        List<String> list = new LinkedList<>();
        Enumeration enumeration = NetworkInterface.getNetworkInterfaces();
        while (enumeration.hasMoreElements()) {
            NetworkInterface network = (NetworkInterface) enumeration.nextElement();
            if (network.isVirtual() || !network.isUp()) {
                continue;
            } else {
                Enumeration addresses = network.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = (InetAddress) addresses.nextElement();
                    if (address != null && (address instanceof Inet4Address || address instanceof Inet6Address)) {
                        list.add(address.getHostAddress());
                    }
                }
            }
        }
        for (String s : list) {
            if (!s.contains("127")&&!s.contains("localhost")&&!s.contains("0:0:0:0:0:0:0:1")&&!s.contains("192.168")&&!s.contains(":")) {
                System.out.println(s);
                return s;
            }
        }
        return list.get(0);
    }

    public static void joinInZookeeper() throws Exception{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework client =
            CuratorFrameworkFactory.newClient(ZookeeperIP,retryPolicy);
        client.start();
        MasterSocket = new Socket(MasterIP,8086);
        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/Distributed_Minisql/czr", ipadd().getBytes());
        new Thread(new task(MasterSocket)).start();
        //client.close();
    }

    // 返回本机IP地址字符串
    public static String getHostAddress() {
        try{
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()){
                NetworkInterface netInterface = allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()){
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")){
                        return ip.getHostAddress();
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }


    static class task implements Runnable{
        private Socket socket;
        public task(Socket socket){
            this.socket = socket;
        }
        public void run(){
            try{
                System.out.println("Start");
                BufferedReader in = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                BufferedWriter out = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
                System.out.println("connect");
                interpret(in,out);
                System.out.println("close");
                socket.close();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    private static void interpret(BufferedReader in , BufferedWriter out) throws Exception{
        while(true){
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = in.readLine()) != null){
                System.out.println(line);
                if(line.equals("end")){
                    break;
                }
                sb.append(line);
            }
            String request = sb.toString();
            System.out.println(request);
            String method = request.split(":")[0];
            switch(method){
                case "region":
                    String tablename = request.split(":")[1];
                    String region1Ip = request.split(":")[3];
                    ArrayList<String> region = new ArrayList<String>();
                    region.add(region1Ip);
                    regionMap.put(tablename, region);
                    break;


                case "execute_backup":                        //执行sql语句
                    System.out.println("execute");
                    if(request.split(":").length == 1){
                        out.write("error: no sql");
                        out.newLine();
                        out.write("end");
                        out.newLine();
                        out.flush();
                        break;
                    }
                    String tmp_backup = request.split(":")[1];
                    String sql_backup = tmp_backup.trim().replaceAll("\\s+", " ");

                    String [] sql_array_backup = sql_backup.split(";");
                    for(Integer i = 0; i < sql_array_backup.length; i++){
                        System.out.println(sql_array_backup[i]);
                        excuteSql(sql_array_backup[i],in,out,false);
                    }
                    out.write("end");
                    out.newLine();
                    out.flush();
                    break;

                case "execute":                        //执行sql语句
                    System.out.println("execute");
                    if(request.split(":").length == 1){
                        out.write("error: no sql");
                        out.newLine();
                        out.write("end");
                        out.newLine();
                        out.flush();
                        break;
                    }
                    String tmp = request.split(":")[1];
                    String sql = tmp.trim().replaceAll("\\s+", " ");

                    String [] sql_array = sql.split(";");
                    for(Integer i = 0; i < sql_array.length; i++){
                        System.out.println(sql_array[i]);
                        excuteSql(sql_array[i],in,out);
                    }
                    out.write("end");
                    out.newLine();
                    out.flush();
                    break;

                case "detect":                        //检测可用性
                    System.out.println("detect");
                    out.write("ok");
                    out.newLine();
                    out.write("end");
                    out.newLine();
                    out.flush();
                    break;

                case "copy":
                    System.out.println("copy");
                    String tableName_copy = request.split(":")[1];
                    String regionName_copy = request.split(":")[2];
                    System.out.print(regionName_copy);
                    Socket socket_copy = new Socket(regionName_copy,8080);
                    BufferedWriter moveOut_copy = new BufferedWriter(new java.io.OutputStreamWriter(socket_copy.getOutputStream()));
                    String filename_copy = "tables/"+tableName_copy+".sql";
                    BufferedReader reader_copy = new BufferedReader(new FileReader(filename_copy));
                    String sql_copy = "";
                    String line1_copy = "";
                    while((line1_copy = reader_copy.readLine()) != null){
                        sql_copy+=line1_copy;
                    }
                    moveOut_copy.write("backup:"+tableName_copy+":"+sql_copy);
                    moveOut_copy.newLine();
                    moveOut_copy.write("end");
                    moveOut_copy.newLine();
                    moveOut_copy.flush();

                    out.write("end");
                    out.newLine();
                    out.flush();
                    break;


                case "move":                          //master提示region移动表
                    System.out.println("move");
                    String tableName = request.split(":")[1];
                    String regionName = request.split(":")[2];
                    System.out.print(regionName);
                    Socket socket = new Socket(regionName,8080);
                    BufferedWriter moveOut = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));
                    moveOut.write("execute_backup:");
                    String filename = "tables/"+tableName+".sql";
                    File file = new File(filename);
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    String line1;
                    while((line1 = reader.readLine()) != null){
                        String[] lineall = line1.split(";");
                        for (int i = 0; i < lineall.length; i++) {
                            String nowsql = lineall[i];
                            if(nowsql.contains("create table")||nowsql.contains("insert into")||nowsql.contains("delete from")||nowsql.contains("update")){
                                moveOut.write(nowsql + ";");
                            }
                        }
                    }
                    moveOut.newLine();
                    moveOut.write("end");
                    moveOut.newLine();
                    moveOut.flush();
                    BufferedReader moveIn = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                    String line2;
                    while((line2 = moveIn.readLine()) != null){
                        if(line2.equals("end")){
                            break;
                        }
                        System.out.println(line2);
                    }
                    out.write("success");
                    out.newLine();
                    out.write("end");
                    out.newLine();
                    out.flush();
                    break;

                case "backup":                        //备份
                    System.out.println("backup");
                    String tableName1 = request.split(":")[1];
                    String backupSql = request.split(":")[2];
                    String filename1 = "tables/"+tableName1+".sql";
                    FileWriter writer = new FileWriter(filename1, true);
                    String[] lineall = backupSql.split(";");
                    for (int i = 0; i < lineall.length; i++) {
                        String nowsql = lineall[i];
                        writer.write(nowsql + ";\n");
                    }
                    writer.close();
                    break;

                case "quit":
                    System.out.println("quit");
                    out.write("bye");
                    out.newLine();
                    out.write("end");
                    out.newLine();
                    out.flush();
                    return;

                default:
                    System.out.println("UNKNOWN");
                    out.write("UNKNOWN");
                    out.newLine();
                    out.write("end");
                    out.newLine();
                    out.flush();
                    break;
            }
        }
    }

    private static void excuteSql(String sql,BufferedReader reader, BufferedWriter writer, Boolean needbackup) throws IOException {
        String [] tokens = sql.split(" ");
        try {
            if (tokens.length == 1 && tokens[0].equals(""))
                throw new QException(0, 200, "No statement specified");
            switch (tokens[0]) { //match keyword
                case "create":
                    if (tokens.length == 1)
                        throw new QException(0, 201, "Can't find create object");
                    switch (tokens[1]) {
                        case "table":
                            parse_create_table(sql, writer);
                            break;
                        case "index":
                            parse_create_index(sql, writer);
                            break;
                        default:
                            throw new QException(0, 202, "Can't identify " + tokens[1]);
                    }
                    break;
                case "drop":
                    if (tokens.length == 1)
                        throw new QException(0, 203, "Can't find drop object");
                    switch (tokens[1]) {
                        case "table":
                            parse_drop_table(sql, writer);
                            break;

                        case "index":
                            parse_drop_index(sql, writer);
                            break;

                        default:
                            throw new QException(0, 204, "Can't identify " + tokens[1]);
                    }
                    break;
                case "select":
                    parse_select(sql, writer);
                    break;
                case "insert":
                    parse_insert(sql, writer);
                    break;
                case "delete":
                    parse_delete(sql, writer);
                    break;
                default:
                    throw new QException(0, 205, "Can't identify " + tokens[0]);
            }
            if(!tokens[0].equals("select")){                   //不是select语句的全部备份
                String filename = "tables/"+tokens[2]+".sql";
                FileWriter filewriter = new FileWriter(filename, true);
                filewriter.write(sql + ";\n");
                filewriter.close();
            }
        } catch (QException e) {
            System.out.println(e.status + " " + QException.ex[e.type] + ": " + e.msg);
            writer.write(e.status + " " + QException.ex[e.type] + ": " + e.msg);
            writer.newLine();
            writer.flush();
        } catch (Exception e) {
            System.out.println("Default error: " + e.getMessage());
            writer.write("Default error: " + e.getMessage());
            writer.newLine();
            writer.flush();
        }
    }

    private static void excuteSql(String sql,BufferedReader reader, BufferedWriter writer) throws IOException {
        String [] tokens = sql.split(" ");
        try {
            if (tokens.length == 1 && tokens[0].equals(""))
                throw new QException(0, 200, "No statement specified");
            switch (tokens[0]) { //match keyword
                case "create":
                    if (tokens.length == 1)
                        throw new QException(0, 201, "Can't find create object");
                    switch (tokens[1]) {
                        case "table":
                            parse_create_table(sql, writer);
                            break;
                        case "index":
                            parse_create_index(sql, writer);
                            break;
                        default:
                            throw new QException(0, 202, "Can't identify " + tokens[1]);
                    }
                    break;
                case "drop":
                    if (tokens.length == 1)
                        throw new QException(0, 203, "Can't find drop object");
                    switch (tokens[1]) {
                        case "table":
                            parse_drop_table(sql, writer);
                            break;

                        case "index":
                            parse_drop_index(sql, writer);
                            break;

                        default:
                            throw new QException(0, 204, "Can't identify " + tokens[1]);
                    }
                    break;
                case "select":
                    parse_select(sql, writer);
                    break;
                case "insert":
                    parse_insert(sql, writer);
                    break;
                case "delete":
                    parse_delete(sql, writer);
                    break;
                default:
                    throw new QException(0, 205, "Can't identify " + tokens[0]);
            }
            if(!tokens[0].equals("select")){                   //不是select语句的全部备份
                String filename = "tables/"+tokens[2]+".sql";
                FileWriter filewriter = new FileWriter(filename, true);
                filewriter.write(sql + ";\n");
                filewriter.close();

                String region1Ip = regionMap.get(tokens[2]).get(0);
                System.out.println(region1Ip);
                // String region2Ip = regionMap.get(tokens[2]).get(1);

                Socket socket1 = new Socket(region1Ip,8080);
                System.out.println("Success connect to region1");
                BufferedWriter out1 = new BufferedWriter(new java.io.OutputStreamWriter(socket1.getOutputStream()));
                BufferedReader in1 = new BufferedReader(new java.io.InputStreamReader(socket1.getInputStream()));
                System.out.println("ready to send");
                out1.write("backup:"+tokens[2]+":"+sql+";");
                out1.newLine();
                System.out.println("end to send");
                out1.write("end");
                out1.newLine();
                out1.flush();

                if(in1.readLine().equals("success")){
                    System.out.println("success");
                    writer.write("success");
                    writer.newLine();
                    writer.flush();
                }
                socket1.close();
            }
        } catch (QException e) {
            System.out.println(e.status + " " + QException.ex[e.type] + ": " + e.msg);
            writer.write(e.status + " " + QException.ex[e.type] + ": " + e.msg);
            writer.newLine();
            writer.flush();
        } catch (Exception e) {
            System.out.println("Default error: " + e.getMessage());
            writer.write("Default error: " + e.getMessage());
            writer.newLine();
            writer.flush();
        }
    }

    private static void parse_create_table(String statement ,BufferedWriter writer) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^create table", "").trim(); //skip create table keyword

        int startIndex, endIndex;
        if (statement.equals("")) //no statement after create table
            throw new QException(0, 401, "Must specify a table name");

        endIndex = statement.indexOf(" ");
        if (endIndex == -1)  //no statement after create table xxx
            throw new QException(0, 402, "Can't find attribute definition");

        String tableName = statement.substring(0, endIndex); //get table name
        startIndex = endIndex + 1; //start index of '('
        if (!statement.substring(startIndex).matches("^\\(.*\\)$"))  //check brackets
            throw new QException(0, 403, "Can't not find the definition brackets in table " + tableName);

        int length;
        String[] attrParas, attrsDefine;
        String attrName, attrType, attrLength = "", primaryName = "";
        boolean attrUnique;
        Attribute attribute;
        Vector<Attribute> attrVec = new Vector<>();

        attrsDefine = statement.substring(startIndex + 1).split(","); //get each attribute definition
        for (int i = 0; i < attrsDefine.length; i++) { //for each attribute
            if (i == attrsDefine.length - 1) { //last line
                attrParas = attrsDefine[i].trim().substring(0, attrsDefine[i].length() - 1).split(" "); //remove last ')'
            } else {
                attrParas = attrsDefine[i].trim().split(" ");
            } //split each attribute in parameters: name, type,（length) (unique)

            if (attrParas[0].equals("")) { //empty
                throw new QException(0, 404, "Empty attribute in table " + tableName);
            } else if (attrParas[0].equals("primary")) { //primary key definition
                if (attrParas.length != 3 || !attrParas[1].equals("key"))  //not as primary key xxxx
                    throw new QException(0, 405, "Error definition of primary key in table " + tableName);
                if (!attrParas[2].matches("^\\(.*\\)$"))  //not as primary key (xxxx)
                    throw new QException(0, 406, "Error definition of primary key in table " + tableName);
                if (!primaryName.equals("")) //already set primary key
                    throw new QException(0, 407, "Redefinition of primary key in table " + tableName);

                primaryName = attrParas[2].substring(1, attrParas[2].length() - 1); //set primary key
            } else { //ordinary definition
                if (attrParas.length == 1)  //only attribute name
                    throw new QException(0, 408, "Incomplete definition in attribute " + attrParas[0]);
                attrName = attrParas[0]; //get attribute name
                attrType = attrParas[1]; //get attribute type
                for (int j = 0; j < attrVec.size(); j++) { //check whether name redefines
                    if (attrName.equals(attrVec.get(j).attributeName))
                        throw new QException(0, 409, "Redefinition in attribute " + attrParas[0]);
                }
                if (attrType.equals("int") || attrType.equals("float")) { //check type
                    endIndex = 2; //expected end index
                } else if (attrType.equals("char")) {
                    if (attrParas.length == 2)  //no char length
                        throw new QException(0, 410, "ust specify char length in " + attrParas[0]);
                    if (!attrParas[2].matches("^\\(.*\\)$"))  //not in char (x) form
                        throw new QException(0, 411, "Wrong definition of char length in " + attrParas[0]);

                    attrLength = attrParas[2].substring(1, attrParas[2].length() - 1); //get length
                    try {
                        length = Integer.parseInt(attrLength); //check the length
                    } catch (NumberFormatException e) {
                        throw new QException(0, 412, "The char length in " + attrParas[0] + " dosen't match a int type or overflow");
                    }
                    if (length < 1 || length > 255)
                        throw new QException(0, 413, "The char length in " + attrParas[0] + " must be in [1,255] ");
                    endIndex = 3; //expected end index
                } else { //unmatched type
                    throw new QException(0, 414, "Error attribute type " + attrType + " in " + attrParas[0]);
                }

                if (attrParas.length == endIndex) { //check unique constraint
                    attrUnique = false;
                } else if (attrParas.length == endIndex + 1 && attrParas[endIndex].equals("unique")) {  //unique
                    attrUnique = true;
                } else { //wrong definition
                    throw new QException(0, 415, "Error constraint definition in " + attrParas[0]);
                }

                if (attrType.equals("char")) { //generate attribute
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), Integer.parseInt(attrLength), attrUnique);
                } else {
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), attrUnique);
                }
                attrVec.add(attribute);
            }
        }

        if (primaryName.equals(""))  //check whether set the primary key
            throw new QException(0, 416, "Not specified primary key in table " + tableName);

        Table table = new Table(tableName, primaryName, attrVec); // create table
        API.create_table(tableName, table);
        System.out.println("-->Create table " + tableName + " successfully");
        writer.write("-->Create table " + tableName + " successfully");
        writer.newLine();
        writer.flush();
    }

    private static void parse_drop_table(String statement, BufferedWriter writer) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 601, "Not specify table name");
        if (tokens.length != 3)
            throw new QException(0, 602, "Extra parameters in drop table");

        String tableName = tokens[2]; //get table name
        API.drop_table(tableName);
        System.out.println("-->Drop table " + tableName + " successfully");
        writer.write("-->Drop table " + tableName + " successfully");
        writer.newLine();
        writer.flush();
    }

    private static void parse_create_index(String statement, BufferedWriter writer) throws Exception {
        statement = statement.replaceAll("\\s+", " ");
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.trim();

        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 701, "Not specify index name");

        String indexName = tokens[2]; //get index name
        if (tokens.length == 3 || !tokens[3].equals("on"))
            throw new QException(0, 702, "Must add keyword 'on' after index name " + indexName);
        if (tokens.length == 4)
            throw new QException(0, 703, "Not specify table name");

        String tableName = tokens[4]; //get table name
        if (tokens.length == 5)
            throw new QException(0, 704, "Not specify attribute name in table " + tableName);

        String attrName = tokens[5];
        if (!attrName.matches("^\\(.*\\)$"))  //not as (xxx) form
            throw new QException(0, 705, "Error in specifiy attribute name " + attrName);

        attrName = attrName.substring(1, attrName.length() - 1); //extract attribute name
        if (tokens.length != 6)
            throw new QException(0, 706, "Extra parameters in create index");
        if (!CatalogManager.is_unique(tableName, attrName))
            throw new QException(1, 707, "Not a unique attribute");

        Index index = new Index(indexName, tableName, attrName);
        API.create_index(index);
        System.out.println("-->Create index " + indexName + " successfully");
        writer.write("-->Create index " + indexName + " successfully");
        writer.newLine();
        writer.flush();
    }

    private static void parse_drop_index(String statement, BufferedWriter writer) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 801, "Not specify index name");
        if (tokens.length != 3)
            throw new QException(0, 802, "Extra parameters in drop index");

        String indexName = tokens[2]; //get table name
        API.drop_index(indexName);
        System.out.println("-->Drop index " + indexName + " successfully");
        writer.write("-->Drop index " + indexName + " successfully");
        writer.newLine();
        writer.flush();
    }

    private static void parse_select(String statement, BufferedWriter writer) throws Exception {
        //select ... from ... where ...
        String attrStr = myUtils.substring(statement, "select ", " from");
        String tabStr = myUtils.substring(statement, "from ", " where");
        String conStr = myUtils.substring(statement, "where ", "");
        Vector<Condition> conditions;
        Vector<String> attrNames;
        long startTime, endTime;
        startTime = System.currentTimeMillis();
        if (attrStr.equals(""))
            throw new QException(0, 250, "Can not find key word 'from' or lack of blank before from!");
        if (attrStr.trim().equals("*")) {
            //select all attributes
            if (tabStr.equals("")) {  // select * from [];
                tabStr = myUtils.substring(statement, "from ", "");
                Vector<TableRow> ret = API.select(tabStr, new Vector<>(), new Vector<>());
                endTime = System.currentTimeMillis();
                try{
                    myUtils.print_rows(ret, tabStr, writer);
                }
                catch (Exception e){
                    throw new QException(0, 251, "Error in select all attributes");
                }

            } else { //select * from [] where [];
                String[] conSet = conStr.split(" *and *");
                //get condition vector
                conditions = myUtils.create_conditon(conSet);
                Vector<TableRow> ret = API.select(tabStr, new Vector<>(), conditions);
                endTime = System.currentTimeMillis();
                myUtils.print_rows(ret, tabStr,writer);
            }
        } else {
            attrNames = myUtils.convert(attrStr.split(" *, *")); //get attributes list
            if (tabStr.equals("")) {  //select [attr] from [];
                tabStr = myUtils.substring(statement, "from ", "");
                Vector<TableRow> ret = API.select(tabStr, attrNames, new Vector<>());
                endTime = System.currentTimeMillis();
                myUtils.print_rows(ret, tabStr,writer);
            } else { //select [attr] from [table] where
                String[] conSet = conStr.split(" *and *");
                //get condition vector
                conditions = myUtils.create_conditon(conSet);
                Vector<TableRow> ret = API.select(tabStr, attrNames, conditions);
                endTime = System.currentTimeMillis();
                myUtils.print_rows(ret, tabStr,writer);
            }
        }
        double usedTime = (endTime - startTime) / 1000.0;
        System.out.println("Finished in " + usedTime + " s");
        writer.write("Finished in " + usedTime + " s");
        writer.newLine();
        writer.flush();
    }

    private static void parse_insert(String statement, BufferedWriter writer) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^insert", "").trim();  //skip insert keyword

        int startIndex, endIndex;
        if (statement.equals(""))
            throw new QException(0, 901, "Must add keyword 'into' after insert ");

        endIndex = statement.indexOf(" "); //check into keyword
        if (endIndex == -1)
            throw new QException(0, 902, "Not specify the table name");
        if (!statement.substring(0, endIndex).equals("into"))
            throw new QException(0, 903, "Must add keyword 'into' after insert");

        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); //check table name
        if (endIndex == -1)
            throw new QException(0, 904, "Not specify the insert value");

        String tableName = statement.substring(startIndex, endIndex); //get table name
        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); //check values keyword
        if (endIndex == -1)
            throw new QException(0, 905, "Syntax error: Not specify the insert value");

        if (!statement.substring(startIndex, endIndex).equals("values"))
            throw new QException(0, 906, "Must add keyword 'values' after table " + tableName);

        startIndex = endIndex + 1;
        if (!statement.substring(startIndex).matches("^\\(.*\\)$"))  //check brackets
            throw new QException(0, 907, "Can't not find the insert brackets in table " + tableName);

        String[] valueParas = statement.substring(startIndex + 1).split(","); //get attribute tokens
        TableRow tableRow = new TableRow();

        for (int i = 0; i < valueParas.length; i++) {
            if (i == valueParas.length - 1)  //last attribute
                valueParas[i] = valueParas[i].substring(0, valueParas[i].length() - 1);
            if (valueParas[i].equals("")) //empty attribute
                throw new QException(0, 908, "Empty attribute value in insert value");
            if (valueParas[i].matches("^\".*\"$") || valueParas[i].matches("^\'.*\'$"))  // extract from '' or " "
                valueParas[i] = valueParas[i].substring(1, valueParas[i].length() - 1);
            tableRow.add_attribute_value(valueParas[i]); //add to table row
        }

        //Check unique attributes
        if (tableRow.get_attribute_size() != CatalogManager.get_attribute_num(tableName))
            throw new QException(1, 909, "Attribute number doesn't match");
        Vector<Attribute> attributes = CatalogManager.get_table(tableName).attributeVector;
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.isUnique) {
                Condition cond = new Condition(attr.attributeName, "=", valueParas[i]);
                if (CatalogManager.is_index_key(tableName, attr.attributeName)) {
                    Index idx = CatalogManager.get_index(CatalogManager.get_index_name(tableName, attr.attributeName));
                    if (IndexManager.select(idx, cond).isEmpty())
                        continue;
                } else {
                    Vector<Condition> conditions = new Vector<>();
                    conditions.add(cond);
                    Vector<TableRow> res = RecordManager.select(tableName, conditions); //Supposed to be empty
                    if (res.isEmpty())
                        continue;
                }
                throw new QException(1, 910, "Duplicate unique key: " + attr.attributeName);
            }
        }

        API.insert_row(tableName, tableRow);
        System.out.println("-->Insert successfully");
        writer.write("-->Insert successfully");
        writer.newLine();
        writer.flush();
    }

    private static void parse_delete(String statement, BufferedWriter writer) throws Exception {
        //delete from [tabName] where []
        int num;
        String tabStr = myUtils.substring(statement, "from ", " where").trim();
        String conStr = myUtils.substring(statement, "where ", "").trim();
        Vector<Condition> conditions;
        Vector<String> attrNames;
        if (tabStr.equals("")) {  //delete from ...
            tabStr = myUtils.substring(statement, "from ", "").trim();
            num = API.delete_row(tabStr, new Vector<>());
            System.out.println("Query ok! " + num + " row(s) are deleted");
            writer.write("Query ok! " + num + " row(s) are deleted");
            writer.newLine();
            writer.write("end");
            writer.newLine();
            writer.flush();
        } else {  //delete from ... where ...
            String[] conSet = conStr.split(" *and *");
            //get condition vector
            conditions = myUtils.create_conditon(conSet);
            num = API.delete_row(tabStr, conditions);
            System.out.println("Query ok! " + num + " row(s) are deleted");
            writer.write("Query ok! " + num + " row(s) are deleted");
            writer.newLine();
            writer.flush();
        }
    }

}

class myUtils {

    public static final int NONEXIST = -1;
    public static final String[] OPERATOR = {"<>", "<=", ">=", "=", "<", ">"};

    public static String substring(String str, String start, String end) {
        String regex = start + "(.*)" + end;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) return matcher.group(1);
        else return "";
    }

    public static <T> Vector<T> convert(T[] array) {
        Vector<T> v = new Vector<>();
        for (int i = 0; i < array.length; i++) v.add(array[i]);
        return v;
    }

    //ab <> 'c' | cab ="fabd"  | k=5  | char= '53' | int = 2
    public static Vector<Condition> create_conditon(String[] conSet) throws Exception {
        Vector<Condition> c = new Vector<>();
        for (int i = 0; i < conSet.length; i++) {
            int index = contains(conSet[i], OPERATOR);
            if (index == NONEXIST) throw new Exception("Syntax error: Invalid conditions " + conSet[i]);
            String attr = substring(conSet[i], "", OPERATOR[index]).trim();
            String value = substring(conSet[i], OPERATOR[index], "").trim().replace("\'", "").replace("\"", "");
            c.add(new Condition(attr, OPERATOR[index], value));
        }
        return c;
    }

    public static boolean check_type(String attr, boolean flag) {
        return true;
    }

    public static int contains(String str, String[] reg) {
        for (int i = 0; i < reg.length; i++) {
            if (str.contains(reg[i])) return i;
        }
        return NONEXIST;
    }

    public static int get_max_attr_length(Vector<TableRow> tab, int index) {
        int len = 0;
        for (int i = 0; i < tab.size(); i++) {
            int v = tab.get(i).get_attribute_value(index).length();
            len = v > len ? v : len;
        }
        return len;
    }

    public static void print_rows(Vector<TableRow> tab, String tabName, BufferedWriter writer) throws Exception {
        if (tab.size() == 0) {
            System.out.println("-->Query ok! 0 rows are selected");
            return;
        }
        int attrSize = tab.get(0).get_attribute_size();
        int cnt = 0;
        System.out.println("mark");
        Vector<Integer> v = new Vector<>(attrSize);
        for (int j = 0; j < attrSize; j++) {
            int len = get_max_attr_length(tab, j);
            String attrName = CatalogManager.get_attribute_name(tabName, j);
            if (attrName.length() > len) len = attrName.length();
            v.add(len);
            String format = "|%-" + len + "s";
            System.out.printf(format, attrName);
            cnt = cnt + len + 1;
        }
        System.out.println("mark");
        cnt++;
        System.out.println("|");
        for (int i = 0; i < cnt; i++) System.out.print("-");
        System.out.println();
        System.out.println("mark");
        List<Map<String, String>> list = new ArrayList<>();
        for (int i = 0; i < tab.size(); i++) {
            TableRow row = tab.get(i);
            Map<String, String> inputParams = new HashMap<String, String>();
            for (int j = 0; j < attrSize; j++) {
                String format = "|%-" + v.get(j) + "s";
                System.out.printf(format, row.get_attribute_value(j));
                String attribute_name = "\"" + CatalogManager.get_attribute_name(tabName, j) + "\"";
                String attribute_value = "\"" + row.get_attribute_value(j) + "\"";
                inputParams.put(attribute_name, attribute_value);
            }
            list.add(inputParams);
            System.out.println("|");
        }
        writer.write(list.toString());
        writer.newLine();
        writer.flush();
        System.out.println("-->Query ok! " + tab.size() + " rows are selected");
    }
}
