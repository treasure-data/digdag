package io.digdag.server.rs.project;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ProjectClearScheduleParam {
    final Set<String> workflowNames;
    final boolean allWorkflows;

    public ProjectClearScheduleParam(List<String> workflowNames, boolean allWorkflows) {
        this.workflowNames = workflowNames.stream().collect(Collectors.toSet());
        this.allWorkflows = allWorkflows;
    }

    public boolean checkClear(String workflow) {
        return allWorkflows || workflowNames.contains(workflow);
    }

    /**
     * Check the list of workflows should be cleared
     * @param targetWorkflowNames
     * @return Map of workflows with value (true:cleared false:no need).
     */
    public Map<String, Boolean> checkClearList(List<String> targetWorkflowNames)
    {
        return targetWorkflowNames.stream().collect(Collectors.toMap(String::toString, (s)-> checkClear(s)));
    }

    public void validateWorkflowNames(List<String> workflows) {
        Set<String> allWorkflowNames = workflows.stream().collect(Collectors.toSet());
        for (String wf : this.workflowNames) {
            if (!allWorkflowNames.contains(wf)) {
                throw new IllegalArgumentException(String.format("%s does not exist", wf));
            }
        }
    }
}
