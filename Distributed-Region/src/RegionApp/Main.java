package RegionApp;

import Components.Region;
import Connection.Connect;

public class Main {
    public static void main(String[] args) throws Exception {
        Connect zookeeper = new Connect("10.162.90.213", 12345, "region");
        zookeeper.connect();
        zookeeper.send("server");
        zookeeper.close();
        new Thread(new Region("10.162.90.213", 12345, 8081)).start();
    }
}
