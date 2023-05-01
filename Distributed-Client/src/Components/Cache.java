package Components;
import java.util.HashMap;
import java.util.Map;

public class Cache {
    private Map<String, RegionInfo> cache;

    public Cache() {
        cache = new HashMap<>();
    }

    public RegionInfo get(String tableName) {
        return cache.get(tableName);
    }

    public RegionInfo remove(String tableName) {
        return cache.remove(tableName);
    }

    public RegionInfo put(String tableName, RegionInfo regionServerInfo) {
        return cache.put(tableName, regionServerInfo);
    }
}