package flink.runtime.rpc;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public class RpcServiceUtils {


    /**
     * Creates a wildcard name symmetric to {@link #createRandomName(String)}.
     *
     * @param prefix prefix of the wildcard name
     * @return wildcard name starting with the prefix
     */
    public static String createWildcardName(String prefix) {
        return prefix + "_*";
    }

}
