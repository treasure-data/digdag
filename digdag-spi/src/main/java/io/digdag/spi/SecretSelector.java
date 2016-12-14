package io.digdag.spi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.immutables.value.Value;

import java.util.regex.Pattern;

import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

@Value.Immutable
@Value.Style(visibility = PACKAGE)
@JsonSerialize(as = ImmutableSecretSelector.class)
@JsonDeserialize(as = ImmutableSecretSelector.class)
public interface SecretSelector
{
    Pattern VALID_PATTERN = Pattern.compile("^(\\w+\\.)*(\\w+|\\*)$");

    String pattern();

    @Value.Check
    default void check()
    {
        Preconditions.checkState(VALID_PATTERN.matcher(pattern()).matches(), "Bad secret selector: '" + pattern() + "'");
    }

    default boolean match(String key)
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "key");
        if (pattern().endsWith("*")) {
            String prefix = pattern().substring(0, pattern().length() - 1);
            return key.startsWith(prefix);
        }
        else {
            return pattern().equals(key);
        }
    }

    static SecretSelector of(String pattern)
    {
        try {
            return builder().pattern(pattern).build();
        }
        catch (IllegalStateException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static Builder builder()
    {
        return ImmutableSecretSelector.builder();
    }

    interface Builder
    {
        Builder pattern(String pattern);

        SecretSelector build();
    }
}
