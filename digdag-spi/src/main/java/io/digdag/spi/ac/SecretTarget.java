package io.digdag.spi.ac;

import org.immutables.value.Value;

@Value.Immutable
public interface SecretTarget
{
    int getSiteId();

    String getName();

    int getProjectId();

    String getProjectName();

    static SecretTarget of(int siteId, String name, int projectId, String projectName)
    {
        return ImmutableSecretTarget.builder()
                .siteId(siteId)
                .name(name)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }
}
