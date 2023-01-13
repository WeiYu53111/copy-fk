package flink.core.plugin;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

import flink.configuration.Configuration;
import flink.util.Preconditions;

/**
 * Interface for plugins. Plugins typically extend this interface in their SPI and the concrete
 * implementations of a service then implement the SPI contract.
 */
//@PublicEvolving
public interface Plugin {

    /**
     * Helper method to get the class loader used to load the plugin. This may be needed for some
     * plugins that use dynamic class loading afterwards the plugin was loaded.
     *
     * @return the class loader used to load the plugin.
     */
    default ClassLoader getClassLoader() {
        return Preconditions.checkNotNull(
                this.getClass().getClassLoader(),
                "%s plugin with null class loader",
                this.getClass().getName());
    }

    /**
     * Optional method for plugins to pick up settings from the configuration.
     *
     * @param config The configuration to apply to the plugin.
     */
    default void configure(Configuration config) {}
}
