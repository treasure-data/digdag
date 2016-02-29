package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import com.google.common.base.Optional;
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
        private final Map<Integer, String> matches;

        public MultipleTaskMatchException(String message, Map<Integer, String> matches)
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
            extends MatchException
    {
        public NoMatchException(String message)
        {
            super(message);
        }
    }

    // some symbols are allowed to be in a task name:
    //   _ - .
    final static Pattern DELIMITER_PATTERN = Pattern.compile(
            "(?![\\_\\-\\/\\.])(?=[\\W])",
            Pattern.UNICODE_CHARACTER_CLASS);

    public static TaskMatchPattern compile(String pattern)
    {
        String[] fragments = DELIMITER_PATTERN.split(pattern, 2);
        switch (fragments.length) {
        case 0:
            // pattern is empty
            throw new SyntaxException("Workflow match pattern is invalid: " + pattern);
        case 1:
            // pattern is a workflow name
            return new TaskMatchPattern(fragments[0], Optional.absent());
        default:
            return new TaskMatchPattern(fragments[0], Optional.of(SubtaskMatchPattern.compile(fragments[1])));
        }
    }

    private final String rootWorkflowName;
    private final Optional<SubtaskMatchPattern> subtaskMatchPattern;

    private TaskMatchPattern(String rootWorkflowName, Optional<SubtaskMatchPattern> subtaskMatchPattern)
    {
        this.rootWorkflowName = rootWorkflowName;
        this.subtaskMatchPattern = subtaskMatchPattern;
    }

    public String getRootWorkflowName()
    {
        return rootWorkflowName;
    }

    public WorkflowDefinition findRootWorkflow(WorkflowDefinitionList sources)
        throws NoMatchException
    {
        return findRootWorkflow(sources.get());
    }

    public <T extends WorkflowDefinition> T findRootWorkflow(List<T> sources)
        throws NoMatchException
    {
        for (T source : sources) {
            if (source.getName().equals(rootWorkflowName)) {
                return source;
            }
        }
        throw new NoMatchException(String.format(
                    "Workflow name '%s' doesn't match with any workflows.", rootWorkflowName));
    }

    public Optional<SubtaskMatchPattern> getSubtaskMatchPattern()
    {
        return subtaskMatchPattern;
    }
}
