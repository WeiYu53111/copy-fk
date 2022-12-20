package flink.flink_core.configuration;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static flink.flink_core.configuration.ConfigurationUtils.canBePrefixMap;
import static flink.flink_core.configuration.ConfigurationUtils.convertToPropertiesPrefixed;

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

    public String getString(ConfigOption<String> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    private Optional<Object> getRawValueFromOption(ConfigOption<?> configOption) {
        return applyWithOption(configOption, this::getRawValue);
    }

    private <T> Optional<T> applyWithOption(
            ConfigOption<?> option, BiFunction<String, Boolean, Optional<T>> applier) {
        final boolean canBePrefixMap = canBePrefixMap(option);
        final Optional<T> valueFromExactKey = applier.apply(option.key(), canBePrefixMap);
        if (valueFromExactKey.isPresent()) {
            return valueFromExactKey;
        } else if (option.hasFallbackKeys()) {
            // try the fallback keys
            for (FallbackKey fallbackKey : option.fallbackKeys()) {
                final Optional<T> valueFromFallbackKey =
                        applier.apply(fallbackKey.getKey(), canBePrefixMap);
                if (valueFromFallbackKey.isPresent()) {
                    //loggingFallback(fallbackKey, option);
                    return valueFromFallbackKey;
                }
            }
        }
        return Optional.empty();
    }

    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        Optional<Object> rawValue = getRawValueFromOption(option);
        Class<?> clazz = option.getClazz();

        try {
            if (option.isList()) {
                return rawValue.map(v -> ConfigurationUtils.convertToList(v, clazz));
            } else {
                return rawValue.map(v -> ConfigurationUtils.convertValue(v, clazz));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not parse value '%s' for key '%s'.",
                            rawValue.map(Object::toString).orElse(""), option.key()),
                    e);
        }
    }


    private Optional<Object> getRawValue(String key, boolean canBePrefixMap) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }

        synchronized (this.confData) {
            final Object valueFromExactKey = this.confData.get(key);
            if (!canBePrefixMap || valueFromExactKey != null) {
                return Optional.ofNullable(valueFromExactKey);
            }
            final Map<String, String> valueFromPrefixMap =
                    convertToPropertiesPrefixed(confData, key);
            if (valueFromPrefixMap.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(valueFromPrefixMap);
        }
    }

    /**
     * Returns the value associated with the given config option as an integer.
     *
     * @param configOption The flink.flink_core.configuration option
     * @return the (default) value associated with the given config option
     */
    //TODO 缺少 注解 @PublicEvolving
    public int getInteger(ConfigOption<Integer> configOption) {
        return getOptional(configOption).orElseGet(configOption::defaultValue);
    }

    /**
     * Returns the value associated with the given config option as an integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The flink.flink_core.configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    //@PublicEvolving
    public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Adds the given value to the flink.flink_core.configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(ConfigOption<String> key, String value) {
        setValueInternal(key.key(), value);
    }


    /**
     * Adds the given value to the flink.flink_core.configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setInteger(ConfigOption<Integer> key, int value) {
        setValueInternal(key.key(), value);
    }

}
