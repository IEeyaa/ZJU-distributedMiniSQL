package Components;

import Connection.MasterConnection;
import Connection.RegionConnection;

public class Client {
    private String MasterIP;
    private int MasterPort;
    private Cache cache;
    private MasterConnection master;
    private RegionConnection region;

    public Client() {
        cache = new Cache();
    }

    public void GetMaster() {
        // TODO: connect to zookeeper and get master info
        MasterIP = "127.0.0.1";
        MasterPort = 8090;
    }

    public void run() {
        // connect to zookeeper and get master info
        try {
            GetMaster();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }
        // connect to master
        master = new MasterConnection(MasterIP, MasterPort);

    }

}
