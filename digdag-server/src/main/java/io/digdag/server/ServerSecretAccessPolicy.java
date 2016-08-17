package io.digdag.server;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.digdag.spi.SecretSelector;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;

import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

@Value.Immutable
@Value.Style(visibility = PACKAGE)
@JsonSerialize(as = ImmutableServerSecretAccessPolicy.class)
@JsonDeserialize(as = ImmutableServerSecretAccessPolicy.class)
interface ServerSecretAccessPolicy
{
    Map<String, OperatorSecretAccessPolicy> operators();

    static Builder builder()
    {
        return ImmutableServerSecretAccessPolicy.builder();
    }

    @Value.Immutable
    @Value.Style(visibility = PACKAGE)
    @JsonSerialize(as = ImmutableOperatorSecretAccessPolicy.class)
    @JsonDeserialize(as = ImmutableOperatorSecretAccessPolicy.class)
    interface OperatorSecretAccessPolicy
    {
        List<SecretSelector> secrets();

        static Builder builder()
        {
            return ImmutableOperatorSecretAccessPolicy.builder();
        }

        interface Builder
        {
            Builder secrets(Iterable<? extends SecretSelector> selectors);

            OperatorSecretAccessPolicy build();
        }

        static OperatorSecretAccessPolicy of(List<SecretSelector> selectors)
        {
            return builder().secrets(selectors).build();
        }
    }

    interface Builder
    {
        Builder putOperators(String key, OperatorSecretAccessPolicy value);

        Builder putOperators(Map.Entry<String, ? extends OperatorSecretAccessPolicy> entry);

        Builder operators(Map<String, ? extends OperatorSecretAccessPolicy> entries);

        Builder putAllOperators(Map<String, ? extends OperatorSecretAccessPolicy> entries);

        Builder from(ServerSecretAccessPolicy policy);

        ServerSecretAccessPolicy build();
    }
}
