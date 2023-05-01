package Util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static String getMethod(String sql) {
        String[] parts = sql.split("\\s+");
        return parts.length > 0 ? parts[0].toLowerCase() : null;
    }

    public static String getTables(String sql) {
        List<String> tables = new ArrayList<>();
        Pattern pattern = Pattern.compile(
                "(?i)(?:\\bfrom\\s+|\\bjoin\\s+|\\bupdate\\s+|\\binto\\s+)(\\w+)|\\b(?:create|drop)\\s+table\\s+(\\w+)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                tables.add(matcher.group(1));
            } else {
                tables.add(matcher.group(2));
            }
        }
        return tables.size() == 1 ? tables.get(0).toLowerCase() : null;
    }
}
