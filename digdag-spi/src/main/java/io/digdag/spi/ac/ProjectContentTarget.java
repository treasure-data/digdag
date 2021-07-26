package io.digdag.spi.ac;

import io.digdag.client.config.Config;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
public interface ProjectContentTarget
{
    ProjectTarget getProjectTarget();

    List<Config> taskConfigs();

    static ProjectContentTarget of(ProjectTarget projectTarget, List<Config> taskConfigs)
    {
        return ImmutableProjectContentTarget.builder()
                .projectTarget(projectTarget)
                .taskConfigs(taskConfigs)
                .build();
    }
}
