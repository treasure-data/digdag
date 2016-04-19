package io.digdag.core.archive;

import java.util.List;
import java.time.ZoneId;
import io.digdag.core.repository.ModelValidator;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

class ProjectFile
{
    private final String projectName;

    private final List<String> workflowFiles;

    private final ZoneId defaultTimeZone;

    private final Config defaultParams;

    private ProjectFile(
            String projectName,
            List<String> workflowFiles,
            ZoneId defaultTimeZone,
            Config defaultParams)
    {
        this.projectName = projectName;
        this.workflowFiles = workflowFiles;
        this.defaultTimeZone = defaultTimeZone;
        this.defaultParams = defaultParams;
        check();
    }

    protected void check()
    {
        ModelValidator.builder()
            .checkProjectName("name", projectName)
            .validate("project", this);
    }

    public static ProjectFile fromConfig(Config config)
    {
        Config copy = config.deepCopy();

        String projectName = copy.get("name", String.class);
        copy.remove("name");

        ZoneId defaultTimeZone = copy.getOptional("timezone", ZoneId.class).or(ZoneId.of("UTC"));
        copy.remove("timezone");

        // TODO if users want to put parameters to a project file, enable this code.
        //Config defaultParams = copy.getNestedOrGetEmpty("params");
        //copy.remove("params");
        Config defaultParams = copy.getFactory().create();

        List<String> workflowFiles = copy.getList("workflows", String.class);
        copy.remove("workflows");

        if (!copy.isEmpty()) {
            throw new ConfigException("Project definition file includes unknown keys: " + copy.getKeys());
        }

        return new ProjectFile(
                projectName,
                workflowFiles,
                defaultTimeZone,
                defaultParams);
    }

    public List<String> getWorkflowFiles()
    {
        return workflowFiles;
    }

    public ZoneId getDefaultTimeZone()
    {
        return defaultTimeZone;
    }

    public Config getDefaultParams()
    {
        return defaultParams;
    }
}
