package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import org.immutables.value.Value;

import static com.google.common.base.Strings.isNullOrEmpty;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonDeserialize(as = ImmutableRestSetSecretRequest.class)
public interface RestSetSecretRequest
{
    String value();

    static RestSetSecretRequest of(String value)
    {
        return ImmutableRestSetSecretRequest.builder().value(value).build();
    }
}
