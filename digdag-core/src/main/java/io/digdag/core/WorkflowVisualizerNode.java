
package io.digdag.core;

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableWorkflowVisualizerNode.class)
@JsonDeserialize(as = ImmutableWorkflowVisualizerNode.class)
public abstract class WorkflowVisualizerNode
{
    public abstract String getName();

    public abstract long getId();

    public abstract Optional<Long> getParentId();

    public abstract List<Long> getUpstreamIds();

    public abstract TaskStateCode getState();

    public static ImmutableWorkflowVisualizerNode.Builder builder()
    {
        return ImmutableWorkflowVisualizerNode.builder();
    }

    public static WorkflowVisualizerNode of(WorkflowTask task)
    {
        return builder()
            .name(task.getName())
            .id(task.getIndex())
            .parentId(task.getParentIndex().transform(it -> (long) it))
            .upstreamIds(task.getUpstreamIndexes().stream().map(it -> (long) it).collect(Collectors.toList()))
            .state(TaskStateCode.BLOCKED)
            .build();
    }

    public static WorkflowVisualizerNode of(StoredTask task)
    {
        String[] nameFragments = task.getFullName().split("(?=\\+|\\.)");
        String name = nameFragments[nameFragments.length-1];
        return builder()
            .name(name)
            .id(task.getId())
            .parentId(task.getParentId())
            .upstreamIds(task.getUpstreams())
            .state(task.getState())
            .build();
    }
}
