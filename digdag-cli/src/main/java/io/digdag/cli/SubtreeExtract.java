package io.digdag.cli;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.workflow.WorkflowTaskList;

class SubtreeExtract
{
    static WorkflowTaskList extractSubtree(WorkflowTaskList tasks, int targetTaskIndex)
    {
        return new SubtreeExtract(tasks, targetTaskIndex).getExtracted();
    }

    private final WorkflowTaskList tasks;
    private final int targetTaskIndex;
    private final List<WorkflowTask> extracted;

    private SubtreeExtract(WorkflowTaskList tasks, int targetTaskIndex)
    {
        this.tasks = tasks;
        this.targetTaskIndex = targetTaskIndex;
        this.extracted = new ArrayList<>();
    }

    private WorkflowTaskList getExtracted()
    {
        extracted.add(tasks.get(0));  // always add the root (=workflow itself) because root must exist

        addSubtasksRecursively(tasks.get(targetTaskIndex));

        Map<Integer, Integer> map = new HashMap<>();
        for (int i=0; i < extracted.size(); i++) {
            map.put(extracted.get(i).getIndex(), i);
        }

        ImmutableList.Builder<WorkflowTask> builder = ImmutableList.builder();
        for (WorkflowTask task : extracted) {
            WorkflowTask indexMapped = new WorkflowTask.Builder().from(task)
                .index(map.get(task.getIndex()))
                .parentIndex(
                        task.getParentIndex().transform(index ->
                            map.containsKey(index) ? map.get(index) : 0
                        ))
                .upstreamIndexes(
                        (task.getIndex() == targetTaskIndex) ?
                        ImmutableList.of() :
                        task.getUpstreamIndexes().stream()
                            .map(map::get)
                            .collect(Collectors.toList()))
                .build();
            builder.add(indexMapped);
        }
        return WorkflowTaskList.of(builder.build());
    }

    private void addSubtasksRecursively(WorkflowTask task)
    {
        if (extracted.stream().anyMatch(t -> t.getIndex() == task.getIndex())) {
            return;
        }
        extracted.add(task);

        // add child tasks recursively
        for (WorkflowTask t : tasks.subList(task.getIndex(), tasks.size())) {
            Optional<Integer> parentIndex = t.getParentIndex();
            if (parentIndex.isPresent() && parentIndex.get() == task.getIndex()) {
                addSubtasksRecursively(t);
            }
        }
    }
}
