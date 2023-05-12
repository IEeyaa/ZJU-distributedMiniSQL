package Components;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import Util.Utils;
import Connection.Connection;

public class Client {
    final private String ZookeeperIP = "10.162.90.213";
    final private int ZookeeperPort = 12345;

    private String MasterIP = null;
    private int MasterPort = -1;

    private Cache cache = null;
    private Scanner scanner = null;

    private Connection zookeeper = null;
    private Connection master = null;
    private Connection region = null;

    public Client() {
        cache = new Cache();
        scanner = new Scanner(System.in);

        // connect to zookeeper and get master info
        try {
            GetMaster();
        } catch (Exception e) {
            System.err.println("Failed to connect to zookeeper: " + e.getMessage());
            System.exit(-1);
        }
    }

    private void GetMaster() {
        zookeeper = new Connection(ZookeeperIP, ZookeeperPort, "zookeeper");
        if (!zookeeper.connect())
            return;
        zookeeper.send("client");

        String res = zookeeper.receive();
        String[] parts = res.split(":");
        if (parts.length != 2) {
            System.out.println(res);
            return;
        }
        MasterIP = parts[0];
        MasterPort = Integer.parseInt(parts[1]);
        zookeeper.close();

        System.out.println("Get Master : " + res);
    }

    private RegionInfo GetRegionInfo(String req) {
        master.send(req);
        // obtain the Region
        String res = master.receive();
        String[] parts = res.split(":");
        if (parts.length != 2) {
            System.err.println("Error: Invalid region " + res);
            return null;
        }
        return new RegionInfo(parts[0], Integer.parseInt(parts[1]));
    }

    private void GetSqlReply(String sql) {
        region.send(sql);
        // Retrieve the Region’s response and display it.
        String res = region.receive();
        System.out.println(res);
        // close the connection
        region.close();
        return;
    }

    public void run() {
        // connect to master
        master = new Connection(MasterIP, MasterPort, "master");
        if (!master.connect())
            return;

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
                                RegionInfo regioninfo = GetRegionInfo("<create>");
                                cache.put(FILE_TABLE, regioninfo);
                                region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                                if (!region.connect())
                                    return;
                                GetSqlReply(FILE_SQL + ";");
                                continue;
                            } else {
                                RegionInfo regioninfo = cache.get(FILE_TABLE);
                                if (regioninfo != null) {
                                    region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                                    if (!region.connect())
                                        return;
                                    GetSqlReply(FILE_SQL + ";");
                                } else {
                                    regioninfo = GetRegionInfo("<get>" + FILE_TABLE);
                                    cache.put(FILE_TABLE, regioninfo);
                                    region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                                    if (!region.connect())
                                        return;
                                    GetSqlReply(FILE_SQL + ";");
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
                    RegionInfo regioninfo = GetRegionInfo("<create>");
                    // update the cache
                    cache.put(TABLE, regioninfo);
                    // connect to the Region and send the SQL
                    region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                    if (!region.connect())
                        return;
                    GetSqlReply(SQL + ";");
                    continue;
                } else {
                    RegionInfo regioninfo = cache.get(TABLE);
                    // if exists in the cache.
                    if (regioninfo != null) {
                        // connect to the Region and send the SQL
                        region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                        if (!region.connect())
                            return;
                        GetSqlReply(SQL + ";");
                    } else {
                        // not exist
                        // Send the table name to the Master
                        regioninfo = GetRegionInfo("<get>" + TABLE);
                        // update the cache
                        cache.put(TABLE, regioninfo);
                        // connect to the Region and send the SQL
                        region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                        if (!region.connect())
                            return;
                        GetSqlReply(SQL + ";");
                    }
                    continue;
                }
            }
        }
    }
}
