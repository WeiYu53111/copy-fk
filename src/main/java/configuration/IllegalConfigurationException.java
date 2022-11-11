package configuration;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 10/27/2022
 */
public class IllegalConfigurationException extends RuntimeException {


    public IllegalConfigurationException(String message) {
        super(message);
    }
}
