package Components;

public class RegionInfo {
    private String ip;
    private String port;

    public RegionInfo(String ip, String port) {
        this.ip = ip;
        this.port = port;
    }

    public String IP() {
        return ip;
    }

    public String Port() {
        return port;
    }
}
