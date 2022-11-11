package configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 10/25/2022
 */
public class Configuration {

    protected final HashMap<String, Object> confData;

    public Configuration() {
        this.confData = new HashMap<>();
    }

    public void addAll(Properties dynamicProperties) {

    }

    public void setString(String key, String value) {
        setValueInternal(key, value);
    }

    private <T> void setValueInternal(String key, T value) {
        setValueInternal(key, value, false);
    }

    <T> void setValueInternal(String key, T value, boolean canBePrefixMap) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.confData) {
            if (canBePrefixMap) {
                removePrefixMap(this.confData, key);
            }
            this.confData.put(key, value);
        }
    }


    static boolean removePrefixMap(Map<String, Object> confData, String key) {
        final List<String> prefixKeys =
                confData.keySet().stream()
                        .filter(candidate -> filterPrefixMapKey(key, candidate))
                        .collect(Collectors.toList());
        prefixKeys.forEach(confData::remove);
        return !prefixKeys.isEmpty();
    }


    public static boolean filterPrefixMapKey(String key, String candidate) {
        final String prefixKey = key + ".";
        return candidate.startsWith(prefixKey);
    }

}
