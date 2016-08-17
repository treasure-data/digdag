package io.digdag.client.api;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SecretValidation
{
    private static final String SECRET_KEY_SEGMENT = "[a-zA-Z]|[a-zA-Z][a-zA-Z0-9_\\-]*[a-zA-Z0-9]";
    private static final Pattern SECRET_KEY_PATTERN = Pattern.compile(
            "^(" + SECRET_KEY_SEGMENT + ")(\\.(" + SECRET_KEY_SEGMENT + "))*$");

    private static final int MAX_SECRET_KEY_LENGTH = 255;
    private static final int MAX_SECRET_VALUE_LENGTH = 1024;

    private SecretValidation()
    {
        throw new UnsupportedOperationException();
    }

    public static boolean isValidSecretKey(String key)
    {
        Preconditions.checkNotNull(key);
        return key.length() <= MAX_SECRET_KEY_LENGTH && SECRET_KEY_PATTERN.matcher(key).matches();
    }

    public static boolean isValidSecretValue(String value)
    {
        Preconditions.checkNotNull(value);
        return value.getBytes(UTF_8).length <= MAX_SECRET_VALUE_LENGTH;
    }

    public static boolean isValidSecret(String key, String value)
    {
        return isValidSecretKey(key) && isValidSecretValue(value);
    }
}
