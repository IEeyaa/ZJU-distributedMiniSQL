package RegionApp;

import Components.Region;

public class Main {
    public static void main(String[] args) throws Exception {
        Region regionServer = new Region("127.0.0.1", 8080);
        regionServer.run();
    }
}
