package util;


import javax.annotation.Nullable;

import static util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public class ExceptionUtils {


    public static <T extends Throwable> T firstOrSuppressed(T newException, @Nullable T previous) {
        checkNotNull(newException, "newException");

        /**
         *        Nullable 类会找不到,maven依赖加入
         *          <dependency>
         *             <groupId>com.google.code.findbugs</groupId>
         *             <artifactId>jsr305</artifactId>
         *         </dependency>
         */
        //TODO maven parent pom中找不到  jsr305 的版本号
        if (previous == null || previous == newException) {
            return newException;
        } else {
            previous.addSuppressed(newException);
            return previous;
        }
    }
}
