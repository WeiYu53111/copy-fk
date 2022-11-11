package rpc;


import configuration.Configuration;
import rpc.exception.RpcLoaderException;
import util.ExceptionUtils;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public interface RpcSystem {






    static RpcSystem load(Configuration configuration) {
        final Iterator<RpcSystemLoader> iterator =
                ServiceLoader.load(RpcSystemLoader.class).iterator();

        Exception loadError = null;
        while (iterator.hasNext()) {
            final RpcSystemLoader next = iterator.next();
            try {
                return next.loadRpcSystem(configuration);
            } catch (Exception e) {
                loadError = ExceptionUtils.firstOrSuppressed(e, loadError);
            }
        }
        throw new RpcLoaderException("Could not load RpcSystem.", loadError);
    }
}
