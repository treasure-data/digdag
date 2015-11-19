package io.digdag.core.workflow;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TaskMatchPattern
{
    public static class MultipleMatchException
        extends Exception
    {
        private final Map<Integer, String> matches;

        public MultipleMatchException(String message, Map<Integer, String> matches)
        {
            super(message);
            this.matches = matches;
        }

        public Map<Integer, String> getMatches()
        {
            return matches;
        }
    }

    public static class NoMatchException
        extends Exception
    {
        public NoMatchException(String message)
        {
            super(message);
        }
    }

    private final String pattern;
    private final Pattern regex;

    public TaskMatchPattern(String pattern)
    {
        this.pattern = pattern;
        this.regex = Pattern.compile(".*" + Pattern.quote(pattern));  // same with String.endsWith
    }

    public int findIndex(List<WorkflowTask> compiledTasks)
        throws MultipleMatchException, NoMatchException
    {
        Map<Integer, String> all = findAll(compiledTasks);
        if (all.size() == 1) {
            return all.keySet().iterator().next();
        }
        else if (all.isEmpty()) {
            throw new NoMatchException(String.format(
                        "Pattern '%s' doesn't match with any tasks.", pattern));
        }
        else {
            throw new MultipleMatchException(String.format(
                        "Pattern '%s' is ambiguous. Matching candidates are %s", pattern, all.values()), all);
        }
    }

    private Map<Integer, String> findAll(List<WorkflowTask> compiledTasks)
    {
        Map<String, WorkflowTask> resolved = new FullNameResolver(compiledTasks).resolve();
        Map<Integer, String> map = new TreeMap<>();
        for (Map.Entry<String, WorkflowTask> pair : resolved.entrySet()) {
            if (regex.matcher(pair.getKey()).matches()) {
                map.put(pair.getValue().getIndex(), pair.getKey());
            }
        }
        return map;
    }

    private static class FullNameResolver
    {
        private final List<Entry> entries;

        public FullNameResolver(List<WorkflowTask> tasks)
        {
            this.entries = tasks
                .stream()
                .map(task -> new Entry(task))
                .collect(Collectors.toList());
        }

        public Map<String, WorkflowTask> resolve()
        {
            ImmutableMap.Builder<String, WorkflowTask> builder = ImmutableMap.builder();
            for (Entry entry : entries) {
                builder.put(entry.getFullName(), entry.getTask());
            }
            return builder.build();
        }

        public class Entry
        {
            private final WorkflowTask task;
            private String fullName;

            public Entry(WorkflowTask task)
            {
                this.task = task;
            }

            public String getFullName()
            {
                if (fullName == null) {
                    if (task.getParentIndex().isPresent()) {
                        fullName = entries.get(task.getParentIndex().get()).getFullName() + task.getName();
                    }
                    else {
                        fullName = task.getName();
                    }
                }
                return fullName;
            }

            public WorkflowTask getTask()
            {
                return task;
            }
        }
    }
}
