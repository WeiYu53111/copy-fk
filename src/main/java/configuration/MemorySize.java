package configuration;

import static util.Preconditions.checkArgument;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class MemorySize implements java.io.Serializable{

    private final long bytes;

    public MemorySize(long bytes) {
        checkArgument(bytes >= 0, "bytes must be >= 0");
        this.bytes = bytes;
    }

    public static MemorySize ofMebiBytes(long mebiBytes) {
        return new MemorySize(mebiBytes << 20);
    }

}
