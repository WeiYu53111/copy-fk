package flink.runtime.blob;

import flink.util.TestLogger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/4/2023
 */
public class BlobKeyTest extends TestLogger {



    /**
     * Verifies that the two given key's are different in total but share the same hash.
     *
     * @param key1 first blob key
     * @param key2 second blob key
     */
    static void verifyKeyDifferentHashEquals(BlobKey key1, BlobKey key2) {
        assertNotEquals(key1, key2);
        assertThat(key1.getHash(), equalTo(key2.getHash()));
    }
}
