import configuration.Configuration;
import configuration.IllegalConfigurationException;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rpc.RpcSystem;

import java.io.*;
import java.util.Properties;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 10/21/2022
 */



public class ClusterEntrypoint {

    private Configuration configuration;

    /***
     * slf4j是一个开源项目，它提供我们一个一致的API来使用不同的日志框架，
     * 比如： java.util.logging，logback，log4j等。slf4j使用户可以在运
     * 行时嵌入他们想使用的日志框架。从名字中可以看出，它其实使用的是facade设计模式来实现的。
     */
    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);
    protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
    static final String FLINK_CONF_FILENAME = "flink-conf.yaml";


    private RpcSystem rpcSystem;


    public ClusterEntrypoint(Configuration configuration) {
        this.configuration = configuration;
    }


    public static void main(String[] args) throws ParseException {


        /**
         *  flink中, 针对多种部署模式  ( Yarn、K8s  )以及运行模式 ( Job 、 Session 、 app)
         *  使用了工厂方法设计摸模式  来获取 configuration.Configuration
         *  针对不同的 模式 会有不同的 工厂类取创建对应  XXXXConfiguration对象
         *  然后会 调用  loadConfiguration 方法获取最终的  Configuration对象
         *
         *  因为我们目前只有这么一个入口类,所以不用那么花里胡哨的直接load
         */
        Configuration configuration = loadConfiguration(args);
        ClusterEntrypoint entrypoint = new ClusterEntrypoint(configuration);
        runClusterEntrypoint(entrypoint);
    }

    /**
     * 解析命令行参数获取配置文件位置
     * @param args
     * @return
     */
    private static Configuration loadConfiguration(String[] args) {

        /**
         * flink中读取配置文件的时候，使用的apache common中的 CommandLineParser 来解析配置文件
         */
        DefaultParser parser = new DefaultParser();
        final Options options = new Options();

        final Option CONFIG_DIR_OPTION =
                Option.builder("c")
                        .longOpt("configDir")
                        .required(true)
                        .hasArg(true)
                        .argName("configuration directory")
                        .desc("Directory which contains the configuration file flink-conf.yml.")
                        .build();


        final Option DYNAMIC_PROPERTY_OPTION =
                Option.builder("D")
                        .argName("property=value")
                        .numberOfArgs(2)
                        .valueSeparator('=')
                        .desc("use value for given property")
                        .build();

        options.addOption(CONFIG_DIR_OPTION);
        options.addOption(DYNAMIC_PROPERTY_OPTION);

        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args, true);
        } catch (ParseException e) {
            LOG.error("Could not parse command line arguments {}.", args, e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }

        final String configDir = commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt());
        final Properties dynamicProperties =
                commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());

        if (configDir == null) {
            throw new IllegalArgumentException(
                    "Given configuration directory is null, cannot load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new IllegalConfigurationException(
                    "The given configuration directory name '"
                            + configDir
                            + "' ("
                            + confDirFile.getAbsolutePath()
                            + ") does not describe an existing directory.");
        }

        // get Flink yaml configuration file
        final File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);

        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException(
                    "The Flink config file '"
                            + yamlConfigFile
                            + "' ("
                            + yamlConfigFile.getAbsolutePath()
                            + ") does not exist.");
        }

        /**
         *  读取 flink-conf.yaml 配置文件
          */

        Configuration configuration = loadYAMLResource(yamlConfigFile);

        if (dynamicProperties != null) {
            configuration.addAll(dynamicProperties);
        }

        return configuration;
    }

    private static Configuration loadYAMLResource(File yamlConfigFile) {
        Configuration config = new Configuration();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new FileInputStream(yamlConfigFile)))) {
            String line;
            int lineNo = 0;
            while((line = reader.readLine() )!= null){
                lineNo ++;

                String[] comments = line.split("#",2);
                String conf = comments[0].trim();

                if(conf.length()>0){
                    String[] kv = conf.split(":",2);

                    if(kv.length == 1){
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + yamlConfigFile
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + yamlConfigFile
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }


                    /**
                     * 如果是敏感key,则在日志中隐藏value值
                     */
                    /*LOG.info(
                            "Loading configuration property: {}, {}",
                            key,
                            isSensitive(key) ? HIDDEN_CONTENT : value);*/
                    config.setString(key, value);

                }

            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }

    private static void runClusterEntrypoint(ClusterEntrypoint entrypoint) {

        /**
         * 启动JobManager
         */
        try {
            entrypoint.startCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startCluster() throws Exception {

        /**
         *  原flink中, 初始化 JobManager中的服务前还有一些准备工作要做的，如下：
         *
         *  1、初始化 PluginManager， 但是没了解过这个具体有些什么用，这里直接省略了
         *  2、权限校验,  UserGroupInformation , 这个还没怎么研究过 Spark中也有这个玩意
         *
         */


        //1、初始化 service 对象, 源代码中 有两个入参,  此处省略了  PluginManager
        initializeServices(configuration);




    }

    protected void initializeServices(Configuration configuration)
            throws Exception {

        LOG.info("Initializing cluster services.");

        /**
         * 1、初始化rpc
         */
        //TODO 补充 RpcSystem 存在的意义


        // 通过java的SPI机制返回RpcSystem的实现类
        rpcSystem = RpcSystem.load(configuration);




    }


}
