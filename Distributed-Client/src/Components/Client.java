package Components;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import Util.Utils;
import Connection.Connection;

public class Client {
    final private String ZookeeperIP = "127.0.0.1";
    final private int ZookeeperPort = 12345;

    private String MasterIP = null;
    private int MasterPort = -1;

    private String RegionIP = null;
    private int RegionPort = -1;

    private Cache cache = null;
    private Scanner scanner = null;

    private Connection zookeeper = null;
    private Connection master = null;
    private Connection region = null;

    public Client() {
        cache = new Cache();
        scanner = new Scanner(System.in);
    }

    private void connectToZookeeper() {
        // connect to zookeeper
        zookeeper = new Connection(ZookeeperIP, ZookeeperPort, "zookeeper");
        while (!zookeeper.connect()) {
            System.err.println("ERROR: Reconnect to zookeeper...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void GetMaster() {
        // send request to zookeeper
        connectToZookeeper();
        zookeeper.send("client");
        String res = zookeeper.receive();
        while (res == null) {
            System.err.println("Error: Zookeeper DIE!");
            connectToZookeeper();
            zookeeper.send("client");
            res = zookeeper.receive();
        }
        String[] parts = res.split(":");
        while (parts.length != 2) {
            System.err.println("Error: Invalid master " + res);
            zookeeper.close();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            connectToZookeeper();
            zookeeper.send("client");
            res = zookeeper.receive();
            parts = res.split(":");
        }
        // get master
        MasterIP = parts[0];
        MasterPort = Integer.parseInt(parts[1]);
        System.out.println("Get Master : " + res);

        zookeeper.close();
    }

    private void connectToMaster() {
        GetMaster();
        master = new Connection(MasterIP, MasterPort, "master");
        while (!master.connect()) {
            System.err.println("ERROR: Reconnect to master...");
            master.close();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            GetMaster();
            master = new Connection(MasterIP, MasterPort, "master");
        }
    }

    private void GetRegion(String req) {
        master.send(req);
        // obtain the Region
        String res = master.receive();
        while (res == null) {
            System.err.println("Error: Master DIE!");
            connectToMaster();
            master.send(req);
            res = master.receive();
        }
        String[] parts = res.split(":");
        while (parts.length != 2) {
            System.err.println("Error: Invalid region " + res);
            master.close();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            connectToMaster();
            master.send(req);
            res = master.receive();
            parts = res.split(":");
        }
        RegionIP = parts[0];
        RegionPort = Integer.parseInt(parts[1]);
    }

    private void connectToRegion(String req) {
        GetRegion(req);
        region = new Connection(RegionIP, RegionPort, "region");
        while (!region.connect()) {
            System.err.println("ERROR: Reconnect to region...");
            region.close();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            GetRegion(req);
            region = new Connection(RegionIP, RegionPort, "region");
        }
    }

    private void GetSqlReply(String sql, String req, String TABLE) {
        region.send(sql);
        // Retrieve the Region’s response and display it.
        String res = region.receive();
        while (res == null) {
            System.err.println("Error: Region DIE!");
            cache.remove(TABLE);
            connectToRegion(req);
            region.send(sql);
            res = region.receive();
        }
        System.out.println(res);
        // update the cache
        cache.put(TABLE, new RegionInfo(RegionIP, RegionPort));
        // close the connection
        region.close();
        return;
    }

    public void run() {
        connectToMaster();
        System.out.println("Welcome to Distributed Database!");
        while (true) {

            System.out.print(">> ");

            // Get the input SQL statement
            StringBuilder sb = new StringBuilder();
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                sb.append(line);
                if (line.endsWith(";"))
                    break;
            }

            // Get sql
            String SQL = Utils.removeTrailingSemicolon(sb.toString());

            // single methods
            if (SQL.equals("quit")) {
                // If it is a quit operation, close the client.
                scanner.close();
                master.send("<quit>");
                master.close();
                break;
            } else if (SQL.equals("cache")) {
                System.out.println(cache.toString());
                continue;
            } else if (SQL.equals("show")) {
                master.send("<show>");
                String res = master.receive();
                while (res == null) {
                    System.err.println("Error: Master DIE!");
                    connectToMaster();
                    master.send("<show>");
                    res = master.receive();
                }
                System.out.println(res);
                continue;
            } else {
                // mutiple methods
                String METHOD = Utils.getMethod(SQL);
                String TABLE = Utils.getTables(SQL);

                if (METHOD.equals("source")) {
                    String file_path = Utils.getFilePath(SQL);
                    File sql_file = new File(file_path);

                    // excute each sql of the file
                    try (BufferedReader fbr = new BufferedReader(new FileReader(sql_file))) {
                        while (true) {
                            String line = fbr.readLine();
                            if (line == null)
                                break;
                            sb = new StringBuilder(line);
                            while (!line.endsWith(";")) {
                                line = fbr.readLine();
                                sb.append(line);
                            }

                            String FILE_SQL = Utils.removeTrailingSemicolon(sb.toString());
                            String FILE_METHOD = Utils.getMethod(FILE_SQL);
                            String FILE_TABLE = Utils.getTables(FILE_SQL);

                            if (FILE_METHOD.equals("create")) {
                                connectToRegion("<create>" + TABLE);
                                GetSqlReply(FILE_SQL + ";", "<create>" + TABLE, FILE_TABLE);
                                continue;
                            } else {
                                RegionInfo regioninfo = cache.get(FILE_TABLE);
                                if (regioninfo != null) {
                                    RegionIP = regioninfo.IP();
                                    RegionPort = regioninfo.Port();
                                    region = new Connection(RegionIP, RegionPort, "region");
                                    if (!region.connect()) {
                                        System.err.println("Error: Region DIE!");
                                        cache.remove(FILE_TABLE);
                                        connectToRegion("<get>" + FILE_TABLE);
                                    }
                                    GetSqlReply(FILE_SQL + ";", "<get>" + FILE_TABLE, FILE_TABLE);
                                } else {
                                    connectToRegion("<get>" + FILE_TABLE);
                                    GetSqlReply(SQL + ";", "<get>" + FILE_TABLE, FILE_TABLE);
                                }
                                continue;
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    continue;
                } else if (METHOD.equals("create")) {
                    // If it is a create operation, send a create request to the Master
                    connectToRegion("<create>" + TABLE);
                    // connect to the Region and send the SQL
                    GetSqlReply(SQL + ";", "<create>" + TABLE, TABLE);
                    continue;
                } else {
                    RegionInfo regioninfo = cache.get(TABLE);
                    // if exists in the cache.
                    if (regioninfo != null) {
                        // connect to the Region and send the SQL
                        RegionIP = regioninfo.IP();
                        RegionPort = regioninfo.Port();
                        region = new Connection(RegionIP, RegionPort, "region");
                        if (!region.connect()) {
                            System.err.println("Error: Region DIE!");
                            cache.remove(TABLE);
                            connectToRegion("<get>" + TABLE);
                        }
                        GetSqlReply(SQL + ";", "<get>" + TABLE, TABLE);
                    } else {
                        // not exist
                        // Send the table name to the Master
                        connectToRegion("<get>" + TABLE);
                        // connect to the Region and send the SQL
                        GetSqlReply(SQL + ";", "<get>" + TABLE, TABLE);
                    }
                    continue;
                }
            }
        }
    }
}
