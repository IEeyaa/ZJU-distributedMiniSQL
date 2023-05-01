package Components;

public class RegionInfo {
    private String ip;
    private int port;

    public RegionInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String IP() {
        return ip;
    }

    public int Port() {
        return port;
    }

    public String toString() {
        return ip + ":" + port;
    }
}
