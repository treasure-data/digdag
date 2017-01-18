package io.digdag.util;

import com.google.common.collect.ImmutableList;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.SecretProvider;

import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserSecretTemplate
{
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{(?<cmd>[\\s\\S]*?):(?<key>[\\s\\S]+?)\\}");

    public static UserSecretTemplate of(String source)
    {
        return new UserSecretTemplate(source);
    }

    private final String source;

    private UserSecretTemplate(String source)
    {
        validateTemplate(source);
        this.source = source;
    }

    private static void validateTemplate(String source)
    {
        Matcher matcher = TEMPLATE_PATTERN.matcher(source);
        while (matcher.find()) {
            if (!"secret".equals(matcher.group("cmd").trim())) {
                throw new ConfigException("Invalid parametrization: '" + matcher.group() + "'");
            }
        }
    }

    public boolean containsSecrets()
    {
        return TEMPLATE_PATTERN.matcher(source).find();
    }

    public List<String> getKeys() {
        ImmutableList.Builder<String> keys = ImmutableList.builder();
        Matcher m = TEMPLATE_PATTERN.matcher(source);
        while (m.find()) {
            keys.add(m.group("key").trim());
        }
        return keys.build();
    }

    public String format(SecretProvider secrets)
    {
        return replaceAll(source, TEMPLATE_PATTERN, (m) -> secrets.getSecret(m.group("key").trim()));
    }

    private static String replaceAll(String source, Pattern pattern, Function<Matcher, String> converter)
    {
        StringBuffer sb = new StringBuffer();

        Matcher m = pattern.matcher(source);
        while (m.find()) {
            m.appendReplacement(sb, converter.apply(m));
        }
        m.appendTail(sb);

        return sb.toString();
    }
}
