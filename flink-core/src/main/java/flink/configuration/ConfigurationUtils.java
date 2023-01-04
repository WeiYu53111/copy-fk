package flink.configuration;



import flink.util.TimeUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/13/2022
 */
public class ConfigurationUtils {

    private static final String[] EMPTY = new String[0];


    // --------------------------------------------------------------------------------------------
    //  Prefix map handling
    // --------------------------------------------------------------------------------------------

    /**
     * Maps can be represented in two ways.
     *
     * <p>With constant key space:
     *
     * <pre>
     *     avro-confluent.properties = schema: 1, other-prop: 2
     * </pre>
     *
     * <p>Or with variable key space (i.e. prefix notation):
     *
     * <pre>
     *     avro-confluent.properties.schema = 1
     *     avro-confluent.properties.other-prop = 2
     * </pre>
     */
    public static boolean canBePrefixMap(ConfigOption<?> configOption) {
        return configOption.getClazz() == Map.class && !configOption.isList();
    }


    // --------------------------------------------------------------------------------------------
    //  Type conversion
    // --------------------------------------------------------------------------------------------

    /**
     * Tries to convert the raw value into the provided type.
     *
     * @param rawValue rawValue to convert into the provided type clazz
     * @param clazz clazz specifying the target type
     * @param <T> type of the result
     * @return the converted value if rawValue is of type clazz
     * @throws IllegalArgumentException if the rawValue cannot be converted in the specified target
     *     type clazz
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (clazz == Duration.class) {
            return (T) convertToDuration(rawValue);
        } else if (clazz == MemorySize.class) {
            return (T) convertToMemorySize(rawValue);
        } else if (clazz == Map.class) {
            return (T) convertToProperties(rawValue);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }


    static <T> T convertToList(Object rawValue, Class<?> atomicClass) {
        if (rawValue instanceof List) {
            return (T) rawValue;
        } else {
            return (T)
                    StructuredOptionsSplitter.splitEscaped(rawValue.toString(), ';').stream()
                            .map(s -> convertValue(s, atomicClass))
                            .collect(Collectors.toList());
        }
    }

    static Map<String, String> convertToProperties(Object o) {
        if (o instanceof Map) {
            return (Map<String, String>) o;
        } else {
            List<String> listOfRawProperties =
                    StructuredOptionsSplitter.splitEscaped(o.toString(), ',');
            return listOfRawProperties.stream()
                    .map(s -> StructuredOptionsSplitter.splitEscaped(s, ':'))
                    .peek(
                            pair -> {
                                if (pair.size() != 2) {
                                    throw new IllegalArgumentException(
                                            "Could not parse pair in the map " + pair);
                                }
                            })
                    .collect(Collectors.toMap(a -> a.get(0), a -> a.get(1)));
        }
    }

    @SuppressWarnings("unchecked")
    static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }

        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    static Duration convertToDuration(Object o) {
        if (o.getClass() == Duration.class) {
            return (Duration) o;
        }

        return TimeUtils.parseDuration(o.toString());
    }

    static MemorySize convertToMemorySize(Object o) {
        if (o.getClass() == MemorySize.class) {
            return (MemorySize) o;
        }

        return MemorySize.parse(o.toString());
    }

    static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        } else if (o.getClass() == Duration.class) {
            Duration duration = (Duration) o;
            return TimeUtils.formatWithHighestUnit(duration);
        } else if (o instanceof List) {
            return ((List<?>) o)
                    .stream()
                    .map(e -> escapeWithSingleQuote(convertToString(e), ";"))
                    .collect(Collectors.joining(";"));
        } else if (o instanceof Map) {
            return ((Map<?, ?>) o)
                    .entrySet().stream()
                    .map(
                            e -> {
                                String escapedKey =
                                        escapeWithSingleQuote(e.getKey().toString(), ":");
                                String escapedValue =
                                        escapeWithSingleQuote(e.getValue().toString(), ":");

                                return escapeWithSingleQuote(
                                        escapedKey + ":" + escapedValue, ",");
                            })
                    .collect(Collectors.joining(","));
        }

        return o.toString();
    }

    static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }


    static Map<String, String> convertToPropertiesPrefixed(
            Map<String, Object> confData, String key) {
        final String prefixKey = key + ".";
        return confData.keySet().stream()
                .filter(k -> k.startsWith(prefixKey))
                .collect(
                        Collectors.toMap(
                                k -> k.substring(prefixKey.length()),
                                k -> convertToString(confData.get(k))));
    }

    /**
     * Escapes the given string with single quotes, if the input string contains a double quote or
     * any of the given {@code charsToEscape}. Any single quotes in the input string will be escaped
     * by doubling.
     *
     * <p>Given that the escapeChar is (;)
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>A,B,C,D => A,B,C,D
     *   <li>A'B'C'D => 'A''B''C''D'
     *   <li>A;BCD => 'A;BCD'
     *   <li>AB"C"D => 'AB"C"D'
     *   <li>AB'"D:B => 'AB''"D:B'
     * </ul>
     *
     * @param string a string which needs to be escaped
     * @param charsToEscape escape chars for the escape conditions
     * @return escaped string by single quote
     */
    static String escapeWithSingleQuote(String string, String... charsToEscape) {
        boolean escape =
                Arrays.stream(charsToEscape).anyMatch(string::contains)
                        || string.contains("\"")
                        || string.contains("'");

        if (escape) {
            return "'" + string.replaceAll("'", "''") + "'";
        }

        return string;
    }


    /**
     * Extracts the task manager directories for temporary files as defined by {@link
     * org.apache.flink.configuration.CoreOptions#TMP_DIRS}.
     *
     * @param configuration configuration object
     * @return array of configured directories (in order)
     */
    @Nonnull
    public static String[] parseTempDirectories(Configuration configuration) {
        return splitPaths(configuration.getString(CoreOptions.TMP_DIRS));
    }

    @Nonnull
    public static String[] splitPaths(@Nonnull String separatedPaths) {
        return separatedPaths.length() > 0
                ? separatedPaths.split(",|" + File.pathSeparator)
                : EMPTY;
    }

}
