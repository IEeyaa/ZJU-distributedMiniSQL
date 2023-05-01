package Components;

public class Client {
    private String MasterIP;
    private int MasterPort;
    private Cache cache;

    public Client() {
        cache = new Cache();
    }

    public void GetMaster() {
        // TODO: get master info from zookeeper
        MasterIP = "localhost";
        MasterPort = 8090;
    }

    public void run() {
        // get master info from zookeeper
        try {
            GetMaster();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
        }
        

    }

}
