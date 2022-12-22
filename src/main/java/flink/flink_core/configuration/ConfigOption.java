package flink.flink_core.configuration;

import flink.flink_core.configuration.description.Description;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static flink.flink_core.util.Preconditions.checkNotNull;


/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class ConfigOption<T> {


    static final Description EMPTY_DESCRIPTION = Description.builder().text("").build();
    // ------------------------------------------------------------------------

    /** The current key for that config option. */
    private final String key;

    /** The default value for this option. */
    private final T defaultValue;

    /** The description for this option. */
    private final Description description;

    /** The list of deprecated keys, in the order to be checked. */
    private final FallbackKey[] fallbackKeys;

    private static final FallbackKey[] EMPTY = new FallbackKey[0];

    /**
     * Type of the value that this ConfigOption describes.
     *
     * <ul>
     *   <li>typeClass == atomic class (e.g. {@code Integer.class}) -> {@code ConfigOption<Integer>}
     *   <li>typeClass == {@code Map.class} -> {@code ConfigOption<Map<String, String>>}
     *   <li>typeClass == atomic class and isList == true for {@code ConfigOption<List<Integer>>}
     * </ul>
     */
    private final Class<?> clazz;

    private final boolean isList;


    /*ConfigOption(
            String key,
            Class<?> clazz,
            Description description,
            T defaultValue,
            boolean isList) {
        this.key = checkNotNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.clazz = checkNotNull(clazz);
        this.isList = isList;
    }*/

    /**
     * Creates a new config option with fallback keys.
     *
     * @param key The current key for that config option
     * @param clazz describes type of the ConfigOption, see description of the clazz field
     * @param description Description for that option
     * @param defaultValue The default value for this option
     * @param isList tells if the ConfigOption describes a list option, see description of the clazz
     *     field
     * @param fallbackKeys The list of fallback keys, in the order to be checked
     */
    ConfigOption(
            String key,
            Class<?> clazz,
            Description description,
            T defaultValue,
            boolean isList,
            FallbackKey... fallbackKeys) {
        this.key = checkNotNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.fallbackKeys = fallbackKeys == null || fallbackKeys.length == 0 ? EMPTY : fallbackKeys;
        this.clazz = checkNotNull(clazz);
        this.isList = isList;
    }


    public ConfigOption<T> withDescription(final String description) {
        return withDescription(Description.builder().text(description).build());
    }

    /**
     * Creates a new config option, using this option's key and default value, and adding the given
     * description. The given description is used when generation the flink.flink_core.configuration documention.
     *
     * @param description The description for this option.
     * @return A new config option, with given description.
     */
    public ConfigOption<T> withDescription(final Description description) {
        return new ConfigOption<>(key, clazz, description, defaultValue, isList, fallbackKeys);
    }

    public T defaultValue() {
        return defaultValue;
    }

    /**
     * Gets the flink.flink_core.configuration key.
     *
     * @return The flink.flink_core.configuration key
     */
    public String key() {
        return key;
    }


    Class<?> getClazz() {
        return clazz;
    }

    boolean isList() {
        return isList;
    }

    /**
     * Checks whether this option has fallback keys.
     *
     * @return True if the option has fallback keys, false if not.
     */
    public boolean hasFallbackKeys() {
        return fallbackKeys != EMPTY;
    }

    /**
     * Gets the fallback keys, in the order to be checked.
     *
     * @return The option's fallback keys.
     */
    public Iterable<FallbackKey> fallbackKeys() {
        return (fallbackKeys == EMPTY) ? Collections.emptyList() : Arrays.asList(fallbackKeys);
    }


    /**
     * Creates a new config option, using this option's key and default value, and adding the given
     * deprecated keys.
     *
     * <p>When obtaining a value from the configuration via {@link
     * Configuration#getValue(ConfigOption)}, the deprecated keys will be checked in the order
     * provided to this method. The first key for which a value is found will be used - that value
     * will be returned.
     *
     * @param deprecatedKeys The deprecated keys, in the order in which they should be checked.
     * @return A new config options, with the given deprecated keys.
     */
    public ConfigOption<T> withDeprecatedKeys(String... deprecatedKeys) {
        final Stream<FallbackKey> newDeprecatedKeys =
                Arrays.stream(deprecatedKeys).map(FallbackKey::createDeprecatedKey);
        final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

        // put deprecated keys last so that they are de-prioritized
        final FallbackKey[] mergedAlternativeKeys =
                Stream.concat(currentAlternativeKeys, newDeprecatedKeys)
                        .toArray(FallbackKey[]::new);
        return new ConfigOption<>(
                key, clazz, description, defaultValue, isList, mergedAlternativeKeys);
    }



    /**
     * Creates a new config option, using this option's key and default value, and adding the given
     * fallback keys.
     *
     * <p>When obtaining a value from the configuration via {@link
     * Configuration#getValue(ConfigOption)}, the fallback keys will be checked in the order
     * provided to this method. The first key for which a value is found will be used - that value
     * will be returned.
     *
     * @param fallbackKeys The fallback keys, in the order in which they should be checked.
     * @return A new config options, with the given fallback keys.
     */
    public ConfigOption<T> withFallbackKeys(String... fallbackKeys) {
        final Stream<FallbackKey> newFallbackKeys =
                Arrays.stream(fallbackKeys).map(FallbackKey::createFallbackKey);
        final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

        // put fallback keys first so that they are prioritized
        final FallbackKey[] mergedAlternativeKeys =
                Stream.concat(newFallbackKeys, currentAlternativeKeys).toArray(FallbackKey[]::new);
        return new ConfigOption<>(
                key, clazz, description, defaultValue, isList, mergedAlternativeKeys);
    }




}
