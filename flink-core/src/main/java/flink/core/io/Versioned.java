package flink.core.io;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

/**
 * This interface is implemented by classes that provide a version number. Versions numbers can be
 * used to differentiate between evolving classes.
 */
//@PublicEvolving
public interface Versioned {

    /**
     * Returns the version number of the object. Versions numbers can be used to differentiate
     * evolving classes.
     */
    int getVersion();
}

