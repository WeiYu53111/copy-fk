package flink.configuration;


import static flink.configuration.ConfigOptions.key;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class CoreOptions {



    /**
     * The config parameter defining the directories for temporary files, separated by ",", "|", or
     * the system's {@link java.io.File#pathSeparator}.
     */
    public static final ConfigOption<String> TMP_DIRS =
            key("io.tmp.dirs")
                    .defaultValue(System.getProperty("java.io.tmpdir"))
                    .withDeprecatedKeys("taskmanager.tmp.dirs")
                    .withDescription(
                            "Directories for temporary files, separated by\",\", \"|\", or the system's java.io.File.pathSeparator.");

}
