package flink.runtime.highavailability.nonha;


import static flink.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public class StandaloneHaServices extends AbstractNonHaServices{


    /** The fix address of the ResourceManager. */
    private final String resourceManagerAddress;

    /** The fix address of the Dispatcher. */
    private final String dispatcherAddress;

    private final String clusterRestEndpointAddress;

    /**
     * Creates a new services class for the fix pre-defined leaders.
     *
     * @param resourceManagerAddress The fix address of the ResourceManager
     * @param clusterRestEndpointAddress
     */
    public StandaloneHaServices(
            String resourceManagerAddress,
            String dispatcherAddress,
            String clusterRestEndpointAddress) {
        this.resourceManagerAddress =
                checkNotNull(resourceManagerAddress, "resourceManagerAddress");
        this.dispatcherAddress = checkNotNull(dispatcherAddress, "dispatcherAddress");
        this.clusterRestEndpointAddress =
                checkNotNull(clusterRestEndpointAddress, clusterRestEndpointAddress);
    }
}
