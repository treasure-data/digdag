package io.digdag.spi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

@Value.Immutable
@Value.Style(visibility = PACKAGE)
@JsonSerialize(as = ImmutableSecretAccessContext.class)
@JsonDeserialize(as = ImmutableSecretAccessContext.class)
public interface SecretAccessContext
{
    int siteId();

    int projectId();

    String revision();

    String workflowName();

    String taskName();

    String operatorType();

    static Builder builder()
    {
        return ImmutableSecretAccessContext.builder();
    }

    interface Builder
    {
        Builder siteId(int siteId);

        Builder projectId(int projectId);

        Builder revision(String revision);

        Builder workflowName(String workflowName);

        Builder taskName(String taskName);

        Builder operatorType(String operatorType);

        SecretAccessContext build();
    }
}
