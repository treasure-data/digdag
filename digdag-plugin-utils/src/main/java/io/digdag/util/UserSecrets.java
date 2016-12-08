package io.digdag.util;

import io.digdag.spi.SecretProvider;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserSecrets
{
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{secret:([\\s\\S]+?)\\}");

    private UserSecrets()
    { }

    public static String userSecretTemplate(String template, SecretProvider secrets)
    {
        return replaceAll(template, TEMPLATE_PATTERN, (m) -> secrets.getSecret(m.group(1).trim()));
    }

    private static String replaceAll(String input, Pattern pattern, Function<Matcher, String> converter)
    {
        StringBuffer sb = new StringBuffer();

        Matcher m = pattern.matcher(input);
        while (m.find()) {
            m.appendReplacement(sb, converter.apply(m));
        }
        m.appendTail(sb);

        return sb.toString();
    }
}
