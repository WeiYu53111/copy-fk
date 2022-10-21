import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 10/21/2022
 */



public class StandaloneSessionClusterEntrypoint {

    /***
     * slf4j是一个开源项目，它提供我们一个一致的API来使用不同的日志框架，
     * 比如： java.util.logging，logback，log4j等。slf4j使用户可以在运
     * 行时嵌入他们想使用的日志框架。从名字中可以看出，它其实使用的是facade设计模式来实现的。
     */
    protected static final Logger LOG = LoggerFactory.getLogger(StandaloneSessionClusterEntrypoint.class);


    public static void main(String[] args) {

        final EntrypointClusterConfiguration entrypointClusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new EntrypointClusterConfigurationParserFactory(),
                        StandaloneSessionClusterEntrypoint.class);
        Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

        StandaloneSessionClusterEntrypoint entrypoint =
                new StandaloneSessionClusterEntrypoint(configuration);

        runClusterEntrypoint(entrypoint);
    }

    private static void runClusterEntrypoint(StandaloneSessionClusterEntrypoint entrypoint) {

    }


}
