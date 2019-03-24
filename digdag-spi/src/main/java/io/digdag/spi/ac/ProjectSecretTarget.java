package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface ProjectSecretTarget
{
    int getSiteId();

    String getSecretName();

    String getProjectName();

    static ProjectSecretTarget of(int siteId, String secretName, String projectName)
    {
        return ImmutableProjectSecretTarget.builder()
                .siteId(siteId)
                .secretName(secretName)
                .projectName(projectName)
                .build();
    }
}
