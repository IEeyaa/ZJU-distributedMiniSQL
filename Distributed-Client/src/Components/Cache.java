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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");

        for (Map.Entry<String, RegionInfo> entry : cache.entrySet()) {
            sb.append("  ");
            sb.append(entry.getKey());
            sb.append(": ");
            sb.append(entry.getValue().toString());
            sb.append(",\n");
        }

        sb.append("}");
        return sb.toString();
    }
}