package io.digdag.core.archive;

import java.util.Map;
import java.time.ZoneId;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import org.immutables.value.Value;
import io.digdag.core.repository.ModelValidator;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public class WorkflowFile
{
    private static final String[] TOP_LEVEL_CONFIG = new String[] {
        "schedule",
        "sla",
        "_error",
        "_check",
        "_retry",
        "_parallel",
    };

    private final String workflowName;

    private final Config tasks;

    private final ZoneId timeZone;

    private final Config topLevelExport;

    private final Config otherTopLevelConfig;

    private WorkflowFile(
            String workflowName,
            Config tasks,
            ZoneId timeZone,
            Config topLevelExport,
            Config otherTopLevelConfig)
    {
        System.out.println("coreのWorkflofileきた ＋＋＋＋＋＋＋＋＋＋＋＋＋＋");
        this.workflowName = workflowName;
        this.tasks = tasks;
        this.timeZone = timeZone;
        this.topLevelExport = topLevelExport;
        this.otherTopLevelConfig = otherTopLevelConfig;
        check();
    }

    public void setBaseWorkdir(String base)
    {
        String workdir = topLevelExport.getOptional("_workdir", String.class)
            .transform(it -> base + "/" + it)  // prepend base to _workdir
            .or(base);
        topLevelExport.set("_workdir", workdir);
    }

    protected void check()
    {
        ModelValidator validator = ModelValidator.builder();
        for (String taskName : tasks.getKeys()) {
            validator.checkTaskName("task name", taskName);
        }
        validator
            .checkWorkflowName("name", workflowName)
            .validate("workflow", this);
        // TODO should here validate key names of defaultParams?
    }

    public static WorkflowFile fromConfig(String workflowName, Config config)
    {
        Config copy = config.deepCopy();

        ZoneId timeZone = copy.getOptional("timezone", ZoneId.class).or(ZoneId.of("UTC"));
        copy.remove("timezone");

        Config topLevelExport = copy.getNestedOrGetEmpty("_export");
        copy.remove("_export");
        System.out.println("fromConfig methodーーーーーーーーーーーーーーーーーーーーーー");
        System.out.println("topLevelExport: " + topLevelExport);

        Config otherTopLevelConfig = copy.getFactory().create();
        for (String key : TOP_LEVEL_CONFIG) {
            if (copy.has(key)) {
                otherTopLevelConfig.set(key, copy.get(key, JsonNode.class));
                copy.remove(key);
            }
        }

        Config tasks = config.getFactory().create();
        for (String key : copy.getKeys()) {
            if (key.startsWith("+")) {
                tasks.set(key, copy.get(key, JsonNode.class));
                copy.remove(key);
            }
        }

        if (!copy.isEmpty()) {
            throw new ConfigException("Workflow '" + workflowName + "' includes unknown keys: " + copy.getKeys());
        }

        return new WorkflowFile(
                workflowName,
                tasks,
                timeZone,
                topLevelExport,
                otherTopLevelConfig);
    }

    public WorkflowDefinition toWorkflowDefinition()
    {
        Config config = tasks.getFactory().create();

        if (!topLevelExport.isEmpty()) {
            config.set("_export", topLevelExport);
        }

        config.setAll(otherTopLevelConfig);

        config.setAll(tasks);

        return WorkflowDefinition.of(workflowName, config, timeZone);
    }
}
