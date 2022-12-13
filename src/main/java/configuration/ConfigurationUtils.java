package configuration;

import java.util.Map;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/13/2022
 */
public class ConfigurationUtils {

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
}
