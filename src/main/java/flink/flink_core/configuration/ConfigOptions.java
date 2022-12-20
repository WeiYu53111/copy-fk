package flink.flink_core.configuration;

import flink.flink_core.configuration.description.Description;

import java.util.Arrays;
import java.util.List;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class ConfigOptions {

    public static OptionBuilder key(String key) {
        checkNotNull(key);
        return new OptionBuilder(key);
    }

    public static class ListConfigOptionBuilder<E> {
        private final String key;
        private final Class<E> clazz;

        ListConfigOptionBuilder(String key, Class<E> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * @param values The list of default values for the config option
         * @return The config option with the default value.
         */
        @SafeVarargs
        public final ConfigOption<List<E>> defaultValues(E... values) {
            return new ConfigOption<>(
                    key, clazz, ConfigOption.EMPTY_DESCRIPTION, Arrays.asList(values), true);
        }

        /**
         * Creates a ConfigOption without a default value.
         *
         * @return The config option without a default value.
         */
        public ConfigOption<List<E>> noDefaultValue() {
            return new ConfigOption<>(key, clazz, ConfigOption.EMPTY_DESCRIPTION, null, true);
        }
    }


    /**
     * Builder for {@link ConfigOption} with a defined atomic type.
     *
     * @param <T> atomic type of the option
     */
    public static class TypedConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        TypedConfigOptionBuilder(String key, Class<T> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        /** Defines that the option's type should be a list of previously defined atomic type. */
        public ListConfigOptionBuilder<T> asList() {
            return new ListConfigOptionBuilder<>(key, clazz);
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * @param value The default value for the config option
         * @return The config option with the default value.
         */
        public ConfigOption<T> defaultValue(T value) {
            return new ConfigOption<>(key, clazz, ConfigOption.EMPTY_DESCRIPTION, value, false);
        }

        /**
         * Creates a ConfigOption without a default value.
         *
         * @return The config option without a default value.
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(
                    key, clazz, Description.builder().text("").build(), null, false);
        }
    }


    public static final class OptionBuilder {

        private final String key;

        OptionBuilder(String key) {
            this.key = key;
        }


        @Deprecated
        public ConfigOption<String> noDefaultValue() {
            return new ConfigOption<>(
                    key, String.class, ConfigOption.EMPTY_DESCRIPTION, null, false);
        }


        @Deprecated
        public <T> ConfigOption<T> defaultValue(T value) {
            checkNotNull(value);
            return new ConfigOption<>(
                    key, value.getClass(), ConfigOption.EMPTY_DESCRIPTION, value, false);
        }

        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(key, String.class);
        }

        /** Defines that the value of the option should be of {@link Integer} type. */
        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(key, Integer.class);
        }


    }
}
