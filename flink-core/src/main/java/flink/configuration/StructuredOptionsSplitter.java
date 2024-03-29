package flink.configuration;

import java.util.ArrayList;
import java.util.List;

import static flink.util.Preconditions.checkNotNull;


/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/14/2022
 */
public class StructuredOptionsSplitter {

    /**
     * Splits the given string on the given delimiter. It supports quoting parts of the string with
     * either single (') or double quotes ("). Quotes can be escaped by doubling the quotes.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>'A;B';C => [A;B], [C]
     *   <li>"AB'D";B;C => [AB'D], [B], [C]
     *   <li>"AB'""D;B";C => [AB'\"D;B], [C]
     * </ul>
     *
     * <p>For more examples check the tests.
     *
     * @param string a string to split
     * @param delimiter delimiter to split on
     * @return a list of splits
     */
    static List<String> splitEscaped(String string, char delimiter) {
        List<Token> tokens = tokenize(checkNotNull(string), delimiter);
        return processTokens(tokens);
    }

    private static List<String> processTokens(List<Token> tokens) {
        final List<String> splits = new ArrayList<>();
        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            switch (token.getTokenType()) {
                case DOUBLE_QUOTED:
                case SINGLE_QUOTED:
                    if (i + 1 < tokens.size()
                            && tokens.get(i + 1).getTokenType() != TokenType.DELIMITER) {
                        int illegalPosition = tokens.get(i + 1).getPosition() - 1;
                        throw new IllegalArgumentException(
                                "Could not split string. Illegal quoting at position: "
                                        + illegalPosition);
                    }
                    splits.add(token.getString());
                    break;
                case UNQUOTED:
                    splits.add(token.getString());
                    break;
                case DELIMITER:
                    if (i + 1 < tokens.size()
                            && tokens.get(i + 1).getTokenType() == TokenType.DELIMITER) {
                        splits.add("");
                    }
                    break;
            }
        }

        return splits;
    }


    /**
     * 截取字符串到builder中,并返回下一个字符的下标
     * @param string
     * @param quote
     * @param cursor
     * @param builder
     * @return
     */
    private static int consumeInQuotes(
            String string, char quote, int cursor, StringBuilder builder) {
        for (int i = cursor + 1; i < string.length(); i++) {
            char c = string.charAt(i);
            if (c == quote) {
                if (i + 1 < string.length() && string.charAt(i + 1) == quote) {
                    builder.append(c);
                    i += 1;
                } else {
                    return i + 1;
                }
            } else {
                builder.append(c);
            }
        }

        throw new IllegalArgumentException(
                "Could not split string. Quoting was not closed properly.");
    }


    private static int consumeUnquoted(
            String string, char delimiter, int cursor, StringBuilder builder) {
        int i;
        for (i = cursor; i < string.length(); i++) {
            char c = string.charAt(i);
            if (c == delimiter) {
                return i;
            }

            builder.append(c);
        }

        return i;
    }

    private static List<Token> tokenize(String string, char delimiter) {
        final List<Token> tokens = new ArrayList<>();
        final StringBuilder builder = new StringBuilder();
        for (int cursor = 0; cursor < string.length(); ) {
            final char c = string.charAt(cursor);

            int nextChar = cursor + 1;
            if (c == '\'') {
                nextChar = consumeInQuotes(string, '\'', cursor, builder);
                tokens.add(new Token(TokenType.SINGLE_QUOTED, builder.toString(), cursor));
            } else if (c == '"') {
                nextChar = consumeInQuotes(string, '"', cursor, builder);
                tokens.add(new Token(TokenType.DOUBLE_QUOTED, builder.toString(), cursor));
            } else if (c == delimiter) {
                tokens.add(new Token(TokenType.DELIMITER, String.valueOf(c), cursor));
            } else if (!Character.isWhitespace(c)) {
                nextChar = consumeUnquoted(string, delimiter, cursor, builder);
                tokens.add(new Token(TokenType.UNQUOTED, builder.toString().trim(), cursor));
            }
            builder.setLength(0);
            cursor = nextChar;
        }

        return tokens;
    }



    private enum TokenType {
        DOUBLE_QUOTED,
        SINGLE_QUOTED,
        UNQUOTED,
        DELIMITER
    }

    private static class Token {
        private final TokenType tokenType;
        private final String string;
        private final int position;

        private Token(TokenType tokenType, String string, int position) {
            this.tokenType = tokenType;
            this.string = string;
            this.position = position;
        }

        public TokenType getTokenType() {
            return tokenType;
        }

        public String getString() {
            return string;
        }

        public int getPosition() {
            return position;
        }
    }

    private StructuredOptionsSplitter() {}
}
