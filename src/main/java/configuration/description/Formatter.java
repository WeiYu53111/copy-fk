package configuration.description;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public abstract class Formatter {

    private final StringBuilder state = new StringBuilder();

    public String format(Description description) {
        for (BlockElement blockElement : description.getBlocks()) {
            blockElement.format(this);
        }
        return finalizeFormatting();
    }

    private String finalizeFormatting() {
        String result = state.toString();
        state.setLength(0);
        return result.replaceAll("%%", "%");
    }

}
