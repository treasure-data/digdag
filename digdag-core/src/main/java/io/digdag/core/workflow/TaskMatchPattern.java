package io.digdag.core.workflow;

import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;

public class TaskMatchPattern
{
    public static class SyntaxException
            extends RuntimeException
    {
        public SyntaxException(String message)
        {
            super(message);
        }
    }

    public static class MatchException
            extends Exception
    {
        public MatchException(String message)
        {
            super(message);
        }
    }

    public static class MultipleTaskMatchException
            extends MatchException
    {
        private final List<String> matches;

        public MultipleTaskMatchException(String message, List<String> matches)
        {
            super(message);
            this.matches = matches;
        }

        public List<String> getMatches()
        {
            return matches;
        }
    }

    public static class NoMatchException
            extends MatchException
    {
        public NoMatchException(String message)
        {
            super(message);
        }
    }

    // some symbols are allowed to be in a task name:
    // allowed: - = [ ] { } % & @ , .
    // See also ModelValidator.RAW_TASK_NAME_CHARS
    final static Pattern DELIMITER_PATTERN = Pattern.compile(
            "(?![\\-\\=\\[\\]\\{\\}\\%\\&\\@\\,\\.\\_])(?=[\\W])",
            Pattern.UNICODE_CHARACTER_CLASS);

    public static TaskMatchPattern compile(String pattern)
    {
        String[] fragments = TaskMatchPattern.DELIMITER_PATTERN.split(pattern);
        if (fragments.length == 0) {
            throw new SyntaxException("Task match pattern is invalid: " + pattern);
        }
        for (String f : fragments) {
            if (f.length() == 1) {
                throw new SyntaxException("Match patterns excepting direct child (+name+name) is not supported: " + pattern);
            }
        }
        return new TaskMatchPattern(pattern, fragments);
    }

    private final String pattern;
    private final Pattern regex;

    // TODO support complex patterns

    private TaskMatchPattern(String pattern, String[] fragments)
    {
        this.pattern = pattern;
        this.regex = Pattern.compile(".*" + Pattern.quote(pattern));  // same with String.endsWith
    }

    public String getPattern()
    {
        return pattern;
    }

    public int findIndex(List<WorkflowTask> tasks)
        throws MultipleTaskMatchException, NoMatchException
    {
        Map<String, WorkflowTask> all = filter(new TaskFullNameResolver(tasks).resolve());
        ensureMatchOne(all.keySet());
        return all.values().iterator().next().getIndex();
    }

    public <T> T find(Map<String, T> tasks)
        throws MultipleTaskMatchException, NoMatchException
    {
        Map<String, T> all = filter(tasks);
        ensureMatchOne(all.keySet());
        return all.values().iterator().next();
    }

    private <T> Map<String, T> filter(Map<String, T> fullNames)
    {
        Map<String, T> map = new LinkedHashMap<>();
        for (Map.Entry<String, T> pair : fullNames.entrySet()) {
            if (regex.matcher(pair.getKey()).matches()) {
                map.put(pair.getKey(), pair.getValue());
            }
        }
        return map;
    }

    private void ensureMatchOne(Collection<String> matchedNames)
        throws MultipleTaskMatchException, NoMatchException
    {
        if (matchedNames.size() == 1) {
            return;
        }
        else if (matchedNames.isEmpty()) {
            throw new NoMatchException(String.format(
                        "Task pattern '%s' doesn't match with any tasks.", pattern));
        }
        else {
            throw new MultipleTaskMatchException(
                    String.format(
                        "Task pattern '%s' is ambiguous. Matching candidates are %s", pattern, matchedNames),
                    ImmutableList.copyOf(matchedNames));
        }
    }

    private static class TaskFullNameResolver
    {
        public static Map<String, WorkflowTask> resolve(List<WorkflowTask> tasks)
        {
            return new TaskFullNameResolver(tasks).resolve();
        }

        private final List<Entry> entries;

        private TaskFullNameResolver(List<WorkflowTask> tasks)
        {
            this.entries = tasks.stream()
                .map(task -> new Entry(task))
                .collect(Collectors.toList());
        }

        private Map<String, WorkflowTask> resolve()
        {
            Map<String, WorkflowTask> map = new LinkedHashMap<>();
            for (Entry entry : entries) {
                map.put(entry.getFullName(), entry.getTask());
            }
            return map;
        }

        private class Entry
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
