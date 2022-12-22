package flink.flink_core.util;

import javax.annotation.Nullable;

/**
 * @Description  一堆用于检查输入的静态方法集合
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public final class Preconditions {

    public static void checkArgument(boolean condition, @Nullable Object errorMessage) {
        if (!condition) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static <T> T checkNotNull(@Nullable T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }


    public static <T> T checkNotNull(@Nullable T reference, @Nullable String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }


    /**
     * Ensures that the given object reference is not null. Upon violation, a {@code
     * NullPointerException} with the given message is thrown.
     *
     * <p>The error message is constructed from a template and an arguments array, after a similar
     * fashion as {@link String#format(String, Object...)}, but supporting only {@code %s} as a
     * placeholder.
     *
     * @param reference The object reference
     * @param errorMessageTemplate The message template for the {@code NullPointerException} that is
     *     thrown if the check fails. The template substitutes its {@code %s} placeholders with the
     *     error message arguments.
     * @param errorMessageArgs The arguments for the error message, to be inserted into the message
     *     template for the {@code %s} placeholders.
     * @return The object reference itself (generically typed).
     * @throws NullPointerException Thrown, if the passed reference was null.
     */
    public static <T> T checkNotNull(
            T reference,
            @Nullable String errorMessageTemplate,
            @Nullable Object... errorMessageArgs) {

        if (reference == null) {
            throw new NullPointerException(format(errorMessageTemplate, errorMessageArgs));
        }
        return reference;
    }


    /**
     * A simplified formatting method. Similar to {@link String#format(String, Object...)}, but with
     * lower overhead (only String parameters, no locale, no format validation).
     *
     * <p>This method is taken quasi verbatim from the Guava Preconditions class.
     */
    private static String format(@Nullable String template, @Nullable Object... args) {
        final int numArgs = args == null ? 0 : args.length;
        template = String.valueOf(template); // null -> "null"

        // start substituting the arguments into the '%s' placeholders
        StringBuilder builder = new StringBuilder(template.length() + 16 * numArgs);
        int templateStart = 0;
        int i = 0;
        while (i < numArgs) {
            int placeholderStart = template.indexOf("%s", templateStart);
            if (placeholderStart == -1) {
                break;
            }
            builder.append(template.substring(templateStart, placeholderStart));
            builder.append(args[i++]);
            templateStart = placeholderStart + 2;
        }
        builder.append(template.substring(templateStart));

        // if we run out of placeholders, append the extra args in square braces
        if (i < numArgs) {
            builder.append(" [");
            builder.append(args[i++]);
            while (i < numArgs) {
                builder.append(", ");
                builder.append(args[i++]);
            }
            builder.append(']');
        }

        return builder.toString();
    }



    /**
     * Checks the given boolean condition, and throws an {@code IllegalStateException} if the
     * condition is not met (evaluates to {@code false}). The exception will have the given error
     * message.
     *
     * @param condition The condition to check
     * @param errorMessage The message for the {@code IllegalStateException} that is thrown if the
     *     check fails.
     * @throws IllegalStateException Thrown, if the condition is violated.
     */
    public static void checkState(boolean condition, @Nullable Object errorMessage) {
        if (!condition) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }
}
