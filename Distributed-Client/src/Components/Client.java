package Components;

import java.util.Scanner;
import java.util.logging.MemoryHandler;

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

    public void GetMaster() {
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

            // Get sql, method name, table name
            String SQL = sb.toString();
            String METHOD = Utils.getMethod(SQL);
            String TABLE = Utils.getTables(SQL);

            // For different methods
            if (METHOD.equals("quit;")) {
                // If it is a quit operation, close the client.
                scanner.close();
                master.send("<quit>");
                master.close();
                break;
            } else if (METHOD.equals("cache;")) {
                System.out.println(cache.toString());
                continue;
            } else if (METHOD.equals("show")) {
                master.send("<show>");
                String res = master.receive();
                System.out.println(res);
                continue;
            } else if (TABLE == null) {
                continue;
            } else if (METHOD.equals("create")) {
                // If it is a create operation, send a create request to the Master
                master.send("<create>");
                // obtain the Region
                String res = master.receive();
                String[] parts = res.split(":");
                if (parts.length != 2)
                    continue;
                RegionInfo regioninfo = new RegionInfo(parts[0], Integer.parseInt(parts[1]));
                // update the cache
                cache.put(TABLE, regioninfo);
                // connect to the Region and send the SQL
                region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                if (!region.connect())
                    return;
                region.send(SQL);
            } else {
                // if exists in the cache.
                RegionInfo regioninfo = cache.get(TABLE);
                if (regioninfo != null) {
                    // connect to the Region and send the SQL
                    region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                    if (!region.connect())
                        return;
                    region.send(SQL);
                } else {
                    // Send the table name to the Master
                    master.send("<get>" + TABLE);
                    // obtain the Region
                    String res = master.receive();
                    String[] parts = res.split(":");
                    if (parts.length != 2)
                        continue;
                    regioninfo = new RegionInfo(parts[0], Integer.parseInt(parts[1]));
                    // update the cache
                    cache.put(TABLE, regioninfo);
                    // connect to the Region and send the SQL
                    region = new Connection(regioninfo.IP(), regioninfo.Port(), "region");
                    if (!region.connect())
                        return;
                    region.send(SQL);
                }
            }
            // Retrieve the Region’s response and display it.
            String res = region.receive();
            System.out.println(res);
            // // close the connection
            // region.send("<close>");
            region.close();
        }
    }
}
