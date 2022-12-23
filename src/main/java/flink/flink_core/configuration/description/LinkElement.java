package flink.flink_core.configuration.description;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/23/2022
 */
public class LinkElement implements InlineElement {

    private final String link;
    private final String text;

    public String getLink() {
        return link;
    }

    public String getText() {
        return text;
    }

    private LinkElement(String link, String text) {
        this.link = link;
        this.text = text;
    }

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }


    /**
     * Creates a link with a given url and description.
     *
     * @param link address that this link should point to
     * @param text a description for that link, that should be used in text
     * @return link representation
     */
    public static LinkElement link(String link, String text) {
        return new LinkElement(link, text);
    }
}
