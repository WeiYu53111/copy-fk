package flink.flink_core.configuration;

import java.util.Locale;
import java.util.Optional;

import static flink.flink_core.configuration.MemorySize.MemoryUnit.*;
import static flink.flink_core.util.Preconditions.checkArgument;
import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class MemorySize implements java.io.Serializable{

    private final long bytes;

    public MemorySize(long bytes) {
        checkArgument(bytes >= 0, "bytes must be >= 0");
        this.bytes = bytes;
    }

    public static MemorySize ofMebiBytes(long mebiBytes) {
        return new MemorySize(mebiBytes << 20);
    }

    /**
     * Parses the given string as as MemorySize.
     *
     * @param text The string to parse
     * @return The parsed MemorySize
     * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
     */
    public static MemorySize parse(String text) throws IllegalArgumentException {
        return new MemorySize(parseBytes(text));
    }

    /**
     * Parses the given string as bytes. The supported expressions are listed under {@link
     * MemorySize}.
     *
     * @param text The string to parse
     * @return The parsed size, in bytes.
     * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
     */
    public static long parseBytes(String text) throws IllegalArgumentException {
        checkNotNull(text, "text");

        final String trimmed = text.trim();
        checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

        final int len = trimmed.length();
        int pos = 0;

        char current;
        while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
            pos++;
        }

        final String number = trimmed.substring(0, pos);
        final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

        if (number.isEmpty()) {
            throw new NumberFormatException("text does not start with a number");
        }

        final long value;
        try {
            value = Long.parseLong(number); // this throws a NumberFormatException on overflow
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "The value '"
                            + number
                            + "' cannot be re represented as 64bit number (numeric overflow).");
        }

        final long multiplier = parseUnit(unit).map(MemoryUnit::getMultiplier).orElse(1L);
        final long result = value * multiplier;

        // check for overflow
        if (result / multiplier != value) {
            throw new IllegalArgumentException(
                    "The value '"
                            + text
                            + "' cannot be re represented as 64bit number of bytes (numeric overflow).");
        }

        return result;
    }

    private static Optional<MemoryUnit> parseUnit(String unit) {
        if (matchesAny(unit, BYTES)) {
            return Optional.of(BYTES);
        } else if (matchesAny(unit, KILO_BYTES)) {
            return Optional.of(KILO_BYTES);
        } else if (matchesAny(unit, MEGA_BYTES)) {
            return Optional.of(MEGA_BYTES);
        } else if (matchesAny(unit, GIGA_BYTES)) {
            return Optional.of(GIGA_BYTES);
        } else if (matchesAny(unit, TERA_BYTES)) {
            return Optional.of(TERA_BYTES);
        } else if (!unit.isEmpty()) {
            throw new IllegalArgumentException(
                    "Memory size unit '"
                            + unit
                            + "' does not match any of the recognized units: "
                            + MemoryUnit.getAllUnits());
        }

        return Optional.empty();
    }

    private static boolean matchesAny(String str, MemoryUnit unit) {
        for (String s : unit.getUnits()) {
            if (s.equals(str)) {
                return true;
            }
        }
        return false;
    }
    /**
     * Enum which defines memory unit, mostly used to parse value from flink.flink_core.configuration file.
     *
     * <p>To make larger values more compact, the common size suffixes are supported:
     *
     * <ul>
     *   <li>1b or 1bytes (bytes)
     *   <li>1k or 1kb or 1kibibytes (interpreted as kibibytes = 1024 bytes)
     *   <li>1m or 1mb or 1mebibytes (interpreted as mebibytes = 1024 kibibytes)
     *   <li>1g or 1gb or 1gibibytes (interpreted as gibibytes = 1024 mebibytes)
     *   <li>1t or 1tb or 1tebibytes (interpreted as tebibytes = 1024 gibibytes)
     * </ul>
     */
    public enum MemoryUnit {
        BYTES(new String[] {"b", "bytes"}, 1L),
        KILO_BYTES(new String[] {"k", "kb", "kibibytes"}, 1024L),
        MEGA_BYTES(new String[] {"m", "mb", "mebibytes"}, 1024L * 1024L),
        GIGA_BYTES(new String[] {"g", "gb", "gibibytes"}, 1024L * 1024L * 1024L),
        TERA_BYTES(new String[] {"t", "tb", "tebibytes"}, 1024L * 1024L * 1024L * 1024L);

        private final String[] units;

        private final long multiplier;

        MemoryUnit(String[] units, long multiplier) {
            this.units = units;
            this.multiplier = multiplier;
        }

        public String[] getUnits() {
            return units;
        }

        public long getMultiplier() {
            return multiplier;
        }

        public static String getAllUnits() {
            return concatenateUnits(
                    BYTES.getUnits(),
                    KILO_BYTES.getUnits(),
                    MEGA_BYTES.getUnits(),
                    GIGA_BYTES.getUnits(),
                    TERA_BYTES.getUnits());
        }

        public static boolean hasUnit(String text) {
            checkNotNull(text, "text");

            final String trimmed = text.trim();
            checkArgument(!trimmed.isEmpty(), "argument is an empty- or whitespace-only string");

            final int len = trimmed.length();
            int pos = 0;

            char current;
            while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
                pos++;
            }

            final String unit = trimmed.substring(pos).trim().toLowerCase(Locale.US);

            return unit.length() > 0;
        }

        private static String concatenateUnits(final String[]... allUnits) {
            final StringBuilder builder = new StringBuilder(128);

            for (String[] units : allUnits) {
                builder.append('(');

                for (String unit : units) {
                    builder.append(unit);
                    builder.append(" | ");
                }

                builder.setLength(builder.length() - 3);
                builder.append(") / ");
            }

            builder.setLength(builder.length() - 3);
            return builder.toString();
        }
    }


}
