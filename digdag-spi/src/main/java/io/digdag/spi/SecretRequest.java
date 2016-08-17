package io.digdag.spi;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public interface SecretRequest
{
    SecretAccessContext context();

    String key();

    static SecretRequest of(SecretAccessContext context, String key)
    {
        return builder().context(context).key(key).build();
    }

    static Builder builder()
    {
        return ImmutableSecretRequest.builder();
    }

    interface Builder
    {
        Builder context(SecretAccessContext context);

        Builder key(String key);

        SecretRequest build();
    }
}
