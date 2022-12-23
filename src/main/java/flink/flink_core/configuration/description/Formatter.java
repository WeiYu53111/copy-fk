package flink.flink_core.configuration.description;

import java.util.EnumSet;

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


    public void format(TextElement element) {
        String[] inlineElements =
                element.getElements().stream()
                        .map(
                                el -> {
                                    Formatter formatter = newInstance();
                                    el.format(formatter);
                                    return formatter.finalizeFormatting();
                                })
                        .toArray(String[]::new);
        formatText(
                state,
                escapeFormatPlaceholder(element.getFormat()),
                inlineElements,
                element.getStyles());
    }

    private String finalizeFormatting() {
        String result = state.toString();
        state.setLength(0);
        return result.replaceAll("%%", "%");
    }


    protected abstract Formatter newInstance();

    protected abstract void formatText(
            StringBuilder state,
            String format,
            String[] elements,
            EnumSet<TextElement.TextStyle> styles);

    private static final String TEMPORARY_PLACEHOLDER = "randomPlaceholderForStringFormat";

    private static String escapeFormatPlaceholder(String value) {
        return value.replaceAll("%s", TEMPORARY_PLACEHOLDER)
                .replaceAll("%", "%%")
                .replaceAll(TEMPORARY_PLACEHOLDER, "%s");
    }


    public void format(LinkElement element) {
        formatLink(state, element.getLink(), element.getText());
    }


    protected abstract void formatLink(StringBuilder state, String link, String description);

}
