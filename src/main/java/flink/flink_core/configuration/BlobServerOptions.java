package flink.flink_core.configuration;

import flink.flink_core.configuration.description.Description;

import static flink.flink_core.configuration.ConfigOptions.key;
import static flink.flink_core.configuration.description.TextElement.code;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class BlobServerOptions {

    /** The config parameter defining the storage directory to be used by the blob server. */
    public static final ConfigOption<String> STORAGE_DIRECTORY =
            key("blob.storage.directory")
                    .noDefaultValue()
                    .withDescription(
                            "The config parameter defining the storage directory to be used by the blob server.");


    /**
     * The config parameter defining the maximum number of concurrent BLOB fetches that the
     * JobManager serves.
     */
    public static final ConfigOption<Integer> FETCH_CONCURRENT =
            key("blob.fetch.num-concurrent")
                    .defaultValue(50)
                    .withDescription(
                            "The config parameter defining the maximum number of concurrent BLOB fetches that the JobManager serves.");



    /** The config parameter defining the backlog of BLOB fetches on the JobManager. */
    public static final ConfigOption<Integer> FETCH_BACKLOG =
            key("blob.fetch.backlog")
                    .defaultValue(1000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The config parameter defining the desired backlog of BLOB fetches on the JobManager."
                                                    + "Note that the operating system usually enforces an upper limit on the backlog size based on the %s setting.",
                                            code("SOMAXCONN"))
                                    .build());


    /**
     * Cleanup interval of the blob caches at the task managers (in seconds).
     *
     * <p>Whenever a job is not referenced at the cache anymore, we set a TTL and let the periodic
     * cleanup task (executed every CLEANUP_INTERVAL seconds) remove its blob files after this TTL
     * has passed. This means that a blob will be retained at most <tt>2 * CLEANUP_INTERVAL</tt>
     * seconds after not being referenced anymore. Therefore, a recovery still has the chance to use
     * existing files rather than to download them again.
     */
    public static final ConfigOption<Long> CLEANUP_INTERVAL =
            key("blob.service.cleanup.interval")
                    .defaultValue(3_600L) // once per hour
                    .withDeprecatedKeys("library-cache-manager.cleanup.interval")
                    .withDescription(
                            "Cleanup interval of the blob caches at the task managers (in seconds).");


    /**
     * The config parameter defining the server port of the blob service. The port can either be a
     * port, such as "9123", a range of ports: "50100-50200" or a list of ranges and or points:
     * "50100-50200,50300-50400,51234"
     *
     * <p>Setting the port to 0 will let the OS choose an available port.
     */
    public static final ConfigOption<String> PORT =
            key("blob.server.port")
                    .defaultValue("0")
                    .withDescription(
                            "The config parameter defining the server port of the blob service.");



    /** Flag to override ssl support for the blob service transport. */
    public static final ConfigOption<Boolean> SSL_ENABLED =
            key("blob.service.ssl.enabled")
                    .defaultValue(true)
                    .withDescription(
                            "Flag to override ssl support for the blob service transport.");

}
