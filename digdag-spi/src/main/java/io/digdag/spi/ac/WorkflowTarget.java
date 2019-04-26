package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface WorkflowTarget
{
    int getSiteId();

    String getName();

    String getProjectName();

    static WorkflowTarget of(int siteId, String name, String projectName)
    {
        return ImmutableWorkflowTarget.builder()
                .siteId(siteId)
                .name(name)
                .projectName(projectName)
                .build();
    }
}
