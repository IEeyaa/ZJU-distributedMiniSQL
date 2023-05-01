import java.util.HashMap;
import java.util.Map;

public class Cache {
    private Map<String, String> cache;

    public Cache() {
        cache = new HashMap<>();
    }

    public String get(String tableName) {
        return cache.get(tableName);
    }

    public String remove(String tableName) {
        return cache.remove(tableName);
    }

    public String put(String tableName, String regionServerInfo) {
        return cache.put(tableName, regionServerInfo);
    }
}
