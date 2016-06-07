package io.digdag.core.plugin;

import java.util.List;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableSpec.class)
@JsonDeserialize(as = ImmutableSpec.class)
public interface Spec
{
    List<String> getRepositories();

    List<String> getDependencies();

    public static Spec of(List<String> repositories, List<String> dependencies)
    {
        return ImmutableSpec.builder()
            .repositories(repositories)
            .dependencies(dependencies)
            .build();
    }
}
