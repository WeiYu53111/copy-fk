package flink.configuration;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class ConfigConstants {
    // ------------------------- JobManager Web Frontend ----------------------

    /**
     * The port for the runtime monitor web-frontend server.
     *
     * @deprecated Use {@link WebOptions#PORT} instead.
     */
    @Deprecated public static final String JOB_MANAGER_WEB_PORT_KEY = "jobmanager.web.port";


    // --------------------------- High Availability ---------------------------------

    /** @deprecated Deprecated in favour of {@link HighAvailabilityOptions#HA_MODE} */
    @Deprecated public static final String DEFAULT_HA_MODE = "none";

    /** @deprecated Deprecated in favour of {@link #DEFAULT_HA_MODE} */
    @Deprecated public static final String DEFAULT_RECOVERY_MODE = "standalone";

}
