package io.digdag.core.workflow;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.session.TaskType;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.repository.ModelValidator;
import io.digdag.core.agent.EditDistance;
import static java.util.Locale.ENGLISH;
import static com.google.common.collect.Maps.immutableEntry;

public class WorkflowCompiler
{
    private static final Set<String> GROUPING_TASK_CONFIG_KEYS = new HashSet<>(ImmutableList.of(
        "timezone",
        "schedule",
        "sla",
        "_parallel",
        "_background",
        "_after",
        "_error",
        "_check",
        "_retry",
        "_export",
        "_secrets"
    ));

    public WorkflowCompiler()
    { }

    public Workflow compile(String name, Config config)
    {
        return Workflow.builder()
            .name(name)
            .meta(config.getNestedOrGetEmpty("meta"))
            .tasks(compileTasks("", "+" + name, config))  // root task name is "+workflowName"
            .build();
    }

    public WorkflowTaskList compileTasks(String parentFullName, String name, Config config)
    {
        return new Context().compile(parentFullName, name, config);
    }

    private static class TaskBuilder
    {
        private final int index;
        private final Optional<TaskBuilder> parent;
        private final String name;
        private final String fullName;
        private final TaskType taskType;
        private final Config config;
        private final List<TaskBuilder> children = new ArrayList<TaskBuilder>();
        private final List<TaskBuilder> upstreams = new ArrayList<TaskBuilder>();

        public TaskBuilder(int index, Optional<TaskBuilder> parent,
                String name, String fullName,
                TaskType taskType, Config config)
        {
            this.index = index;
            this.parent = parent;
            this.name = name;
            this.fullName = fullName;
            this.taskType = taskType;
            this.config = config;
            if (parent.isPresent()) {
                parent.get().addChild(this);
            }
        }

        public int getIndex()
        {
            return index;
        }

        public String getName()
        {
            return name;
        }

        public Config getConfig()
        {
            return config;
        }

        private void addChild(TaskBuilder child)
        {
            children.add(child);
        }

        public void addUpstream(TaskBuilder up)
        {
            upstreams.add(up);
        }

        public WorkflowTask build()
        {
            return new WorkflowTask.Builder()
                .name(name)
                .fullName(fullName)
                .index(index)
                .parentIndex(parent.transform(it -> it.index))
                .upstreamIndexes(
                        upstreams
                            .stream()
                            .map(it -> it.index)
                            .collect(Collectors.toList()))
                .taskType(taskType)
                .config(config)
                .build();
        }
    }

    private static class Context
    {
        private List<TaskBuilder> tasks = new ArrayList<>();

        public WorkflowTaskList compile(String parentFullName, String name, Config config)
        {
            try {
                ModelValidator validator = ModelValidator.builder();
                collect(Optional.absent(), parentFullName, name, config, validator);
                WorkflowTaskList list = WorkflowTaskList.of(
                        tasks
                        .stream()
                        .map(tb -> tb.build())
                        .collect(Collectors.toList()));
                validator.validate("workflow", list);
                return list;
            }
            catch (ConfigException ex) {
                throw ex;
            }
            catch (IllegalStateException ex) {  // thrown by WorkflowTask.check
                throw new ConfigException(ex);
            }
        }

        public TaskBuilder collect(
                Optional<TaskBuilder> parent, String parentFullName,
                String name, Config originalConfig,
                ModelValidator validator)
        {
            Config config = originalConfig.deepCopy();

            // +key: {...}
            List<Entry<String, Config>> subtaskConfigs = config.getKeys()
                .stream()
                .filter(key -> key.startsWith("+"))
                .map(key -> immutableEntry(key, config.getNestedOrderedOrGetEmpty(key)))
                .collect(Collectors.toList());

            // other: ...
            config.getKeys()
                .stream()
                .filter(key -> key.startsWith("+"))
                .forEach(key -> config.remove(key));

            String fullName = parentFullName + name;

            if (config.get("_disable", boolean.class, false)) {
                return addTask(parent, name, fullName, true,
                        config.getFactory().create()
                        .set("_disable", true));
            }
            else if (config.has("_type") || config.getKeys().stream().anyMatch(key -> key.endsWith(">"))) {
                // task node
                if (!subtaskConfigs.isEmpty()) {
                    throw new ConfigException("A task can't have subtasks: " + config);
                }
                if (config.getKeys().stream().filter(key -> key.endsWith(">")).count() > 1) {
                    throw new ConfigException("A task can't have more than one operator: " + config);
                }
                return addTask(parent, name, fullName, false, config);
            }
            else {
                // group node
                TaskBuilder tb = addTask(parent, name, fullName, true, config);

                // validate task names
                subtaskConfigs
                    .stream()
                    .forEach(pair -> {
                        validator.checkRawTaskName("task name", pair.getKey());
                    });

                // validate unused keys
                List<String> unusedKeys = config.getKeys().stream()
                    .filter(key -> !GROUPING_TASK_CONFIG_KEYS.contains(key))
                    .collect(Collectors.toList());
                if (!unusedKeys.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    for (String unusedKey : unusedKeys) {
                        List<String> candidates = EditDistance.suggest(unusedKey, GROUPING_TASK_CONFIG_KEYS, 0.50);
                        if (sb.length() > 0) {
                            sb.append(", ");
                        }
                        if (candidates.isEmpty()) {
                            sb.append(String.format(ENGLISH, "'%s'", unusedKey));
                        }
                        else {
                            sb.append(String.format(ENGLISH,
                                        "'%s' (did you mean %s?)",
                                        unusedKey, candidates.toString()));
                        }
                    }
                    validator.check(fullName, config, unusedKeys.isEmpty(), "contains invalid keys: " + sb.toString());
                }

                List<TaskBuilder> subtasks = subtaskConfigs
                    .stream()
                    .map(pair -> collect(Optional.of(tb), fullName, pair.getKey(), pair.getValue(), validator))
                    .collect(Collectors.toList());

                if (config.get("_parallel", boolean.class, false)) {
                    // _after: is valid only when parallel: is true
                    Map<String, TaskBuilder> names = new HashMap<>();
                    for (TaskBuilder subtask : subtasks) {
                        if (subtask.getConfig().get("_background", boolean.class, false)) {
                            throw new ConfigException("Setting \"_background: true\" option is invalid (unnecessary) is its parent task has \"_parallel: true\" option");
                        }
                        for (String upName : subtask.getConfig().getListOrEmpty("_after", String.class)) {
                            TaskBuilder up = names.get(upName);
                            if (up == null) {
                                throw new ConfigException("Dependency task '"+upName+"' does not exist");
                            }
                            subtask.addUpstream(up);
                        }
                        names.put(subtask.getName(), subtask);
                    }
                }
                else {
                    List<TaskBuilder> beforeList = new ArrayList<>();
                    for (TaskBuilder subtask : subtasks) {
                        if (subtask.getConfig().has("_after")) {
                            throw new ConfigException("Option \"_after\" is valid only if its parent task has \"_parallel: true\"");
                        }
                        if (subtask.getConfig().get("_background", boolean.class, false)) {
                            beforeList.add(subtask);
                        }
                        else {
                            for (TaskBuilder before : beforeList) {
                                subtask.addUpstream(before);
                            }
                            beforeList.clear();
                            beforeList.add(subtask);
                        }
                    }
                }

                return tb;
            }
        }

        private TaskBuilder addTask(
                Optional<TaskBuilder> parent, String name, String fullName,
                boolean groupingOnly, Config config)
        {
            TaskBuilder tb = new TaskBuilder(tasks.size(), parent, name, fullName,
                    extractTaskOption(config, groupingOnly), config);
            tasks.add(tb);
            return tb;
        }

        private TaskType extractTaskOption(Config config, boolean groupingOnly)
        {
            return new TaskType.Builder()
                .groupingOnly(groupingOnly)
                .build();
        }
    }
}
