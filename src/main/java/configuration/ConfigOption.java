package configuration;

import configuration.description.Description;

import static util.Preconditions.checkNotNull;


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


    ConfigOption(
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
    }


    public ConfigOption<T> withDescription(final String description) {
        return withDescription(Description.builder().text(description).build());
    }

    /**
     * Creates a new config option, using this option's key and default value, and adding the given
     * description. The given description is used when generation the configuration documention.
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
     * Gets the configuration key.
     *
     * @return The configuration key
     */
    public String key() {
        return key;
    }

}
