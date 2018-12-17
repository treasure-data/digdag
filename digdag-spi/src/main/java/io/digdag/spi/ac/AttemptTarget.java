package io.digdag.spi.ac;

import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface AttemptTarget
{
    // TODO

    Optional<Integer> getSiteId();

    Optional<Integer> getId();

    Optional<String> getName();

    Optional<Integer> getSessionId();

    Optional<Integer> getProjectId();

    Optional<String> getProjectName();

    public static AttemptTarget of(Optional<Integer> siteId,
            Optional<Integer> id,
            Optional<String> name,
            Optional<Integer> sessionId,
            Optional<Integer> projectId,
            Optional<String> projectName)
    {
        return ImmutableAttemptTarget.builder()
                .siteId(siteId)
                .id(id)
                .name(name)
                .sessionId(sessionId)
                .projectId(projectId)
                .projectName(projectName)
                .build();
    }
}
