package io.digdag.core.workflow;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class SubtaskExtract
{
    public static List<WorkflowTask> extract(List<WorkflowTask> tasks, int rootTaskIndex)
    {
        return new SubtaskExtract(tasks, rootTaskIndex).getExtracted();
    }

    private final List<WorkflowTask> tasks;
    private final int rootTaskIndex;
    private final List<WorkflowTask> extracted;

    private SubtaskExtract(List<WorkflowTask> tasks, int rootTaskIndex)
    {
        this.tasks = tasks;
        this.rootTaskIndex = rootTaskIndex;
        this.extracted = new ArrayList<>();
    }

    public List<WorkflowTask> getExtracted()
    {
        extracted.add(tasks.get(0));  // always add root
        addExtracted(tasks.get(rootTaskIndex));

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
                        (task.getIndex() == rootTaskIndex) ?
                        ImmutableList.of() :
                        task.getUpstreamIndexes().stream()
                            .map(index -> map.get(index))
                            .collect(Collectors.toList()))
                .build();
            builder.add(indexMapped);
        }
        return builder.build();
    }

    public void addExtracted(WorkflowTask task)
    {
        extracted.add(task);

        // add child tasks
        for (WorkflowTask t : tasks.subList(task.getIndex(), tasks.size())) {
            if (t.getParentIndex().isPresent() && t.getParentIndex().get() == task.getIndex()) {
                addExtracted(t);
            }
        }

        // add downstream tasks
        for (WorkflowTask t : tasks.subList(task.getIndex(), tasks.size())) {
            if (t.getUpstreamIndexes().contains(task.getIndex())) {
                addExtracted(t);
            }
        }
    }
}
