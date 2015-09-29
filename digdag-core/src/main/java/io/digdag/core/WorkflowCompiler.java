package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;
import static com.google.common.collect.Maps.immutableEntry;

public class WorkflowCompiler
{
    public WorkflowCompiler()
    { }

    public Workflow compile(String name, ConfigSource config)
    {
        return Workflow.workflowBuilder()
            .name(name)
            .meta(config.getNestedOrGetEmpty("meta"))
            .tasks(compileTasks(name, config))
            .build();
    }

    public List<WorkflowTask> compileTasks(String name, ConfigSource config)
    {
        return new Context().compile(name, config);
    }

    private static class TaskBuilder
    {
        private final int index;
        private final Optional<TaskBuilder> parent;
        private final String name;
        private final WorkflowTaskOptions options;
        private final ConfigSource config;
        private final List<TaskBuilder> children = new ArrayList<TaskBuilder>();
        private final List<TaskBuilder> upstreams = new ArrayList<TaskBuilder>();

        public TaskBuilder(int index, Optional<TaskBuilder> parent, String name,
                WorkflowTaskOptions options, ConfigSource config)
        {
            this.index = index;
            this.parent = parent;
            this.name = name;
            this.options = options;
            this.config = config;
            if (parent.isPresent()) {
                parent.get().addChild(this);
            }
        }

        public String getName()
        {
            return name;
        }

        public ConfigSource getConfig()
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
                .taskIndex(index)
                .parentTaskIndex(
                        parent.transform(it -> it.index))
                .upstreamTaskIndexes(
                        upstreams
                            .stream()
                            .map(it -> it.index)
                            .collect(Collectors.toList()))
                .options(options)
                .config(config)
                .build();
        }
    }

    private static class Context
    {
        private List<TaskBuilder> tasks = new ArrayList<>();

        public List<WorkflowTask> compile(String name, ConfigSource config)
        {
            try {
                collect(Optional.absent(), config.newConfigSource(), name, config);
                return tasks
                    .stream()
                    .map(tb -> tb.build())
                    .collect(Collectors.toList());
            }
            catch (ConfigException ex) {
                throw ex;
            }
            catch (IllegalStateException ex) {  // thrown by WorkflowTask.check
                throw new ConfigException(ex);
            }
        }

        public TaskBuilder collect(
                Optional<TaskBuilder> parent, ConfigSource parentDefaultConfig,
                String name, ConfigSource originalConfig)
        {
            ConfigSource thisDefaultConfig = originalConfig.getNestedOrGetEmpty("default");
            final ConfigSource defaultConfig = parentDefaultConfig.deepCopy().setAll(thisDefaultConfig);
            final ConfigSource config = originalConfig.deepCopy().setAll(defaultConfig);

            // +key: {...}
            List<Entry<String, ConfigSource>> subtaskConfigs = config.getKeys()
                .stream()
                .filter(key -> key.startsWith("+"))
                .map(key -> immutableEntry(key, config.getNested(key)))
                .collect(Collectors.toList());

            // other: ...
            config.getKeys()
                .stream()
                .filter(key -> !key.startsWith("+"))
                .forEach(key -> config.remove(key));

            if (config.has("type") || config.getKeys().stream().anyMatch(key -> key.endsWith(">"))) {
                // task node
                if (subtaskConfigs.isEmpty()) {
                    throw new ConfigException("A task can't have subtasks: " + originalConfig);
                }
                return addTask(parent, name, false, config);
            }
            else {
                // group node
                final TaskBuilder tb = addTask(parent, name, true, config);

                List<TaskBuilder> subtasks = subtaskConfigs
                    .stream()
                    .map(pair -> collect(Optional.of(tb), defaultConfig, pair.getKey(), pair.getValue()))
                    .collect(Collectors.toList());

                if (config.get("parallel", boolean.class, false)) {
                    // after: is valid only when parallel: is true
                    Map<String, TaskBuilder> names = new HashMap<>();
                    for (TaskBuilder subtask : subtasks) {
                        for (String upName : subtask.getConfig().getListOrEmpty("after", String.class)) {
                            TaskBuilder up = names.get(upName);
                            if (up == null) {
                                throw new ConfigException("Dependency task '"+upName+"' does not exist");
                            }
                            tb.addUpstream(up);
                        }
                        names.put(tb.getName(), tb);
                    }
                }
                else {
                    // after: is automatically generated if parallel: is false
                    if (config.has("after")) {
                        throw new ConfigException("Option 'after' is valid only if 'parallel' is true");
                    }
                    TaskBuilder before = null;
                    for (TaskBuilder subtask : subtasks) {
                        if (before != null) {
                            subtask.addUpstream(before);
                        }
                        before = subtask;
                    }
                }

                return tb;
            }
        }

        private TaskBuilder addTask(
                Optional<TaskBuilder> parent, String name,
                boolean groupingOnly, ConfigSource config)
        {
            TaskBuilder tb = new TaskBuilder(tasks.size(), parent, name,
                    extractTaskOption(config, groupingOnly), config);
            tasks.add(tb);
            return tb;
        }

        private WorkflowTaskOptions extractTaskOption(ConfigSource config, boolean groupingOnly)
        {
            return new WorkflowTaskOptions.Builder()
                .groupingOnly(groupingOnly)
                .build();
        }
    }
}
