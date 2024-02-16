package io.digdag.cli;

import java.util.Map;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigException;
import io.digdag.core.agent.LocalWorkspaceManager;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.config.ConfigLoaderManager;
import static java.util.Locale.ENGLISH;
import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;
import static io.digdag.core.archive.ProjectArchive.resourceNameToWorkflowName;

public class Arguments
{
    private static final Logger logger = LoggerFactory.getLogger(Arguments.class);

    private Arguments()
    { }

    public static Config loadParams(ConfigFactory cf,
            ConfigLoaderManager loader,
            Properties systemProps,
            String paramsFile, Map<String, String> params)
        throws IOException
    {
        Config overrideParams = cf.create();

        // ~/.config/digdag/config and JVM system properties
        for (String key : systemProps.stringPropertyNames()) {
            if (key.startsWith("params.")) {
                setDotNestedKey(overrideParams, key.substring("params.".length()), systemProps.getProperty(key));
            }
        }

        // -P files
        if (paramsFile != null) {
            overrideParams.merge(loader.loadParameterizedFile(new File(paramsFile), cf.create()));
        }

        // -p options
        for (Map.Entry<String, String> pair : params.entrySet()) {
            setDotNestedKey(overrideParams, pair.getKey(), pair.getValue());
        }

        return overrideParams;
    }

    private static void setDotNestedKey(Config dest, String key, String value)
    {
        Config nest = dest;
        String[] nestKeys = key.split("\\.");
        for (int i = 0; i < nestKeys.length - 1; i++) {
            try {
                nest = nest.getNestedOrSetEmpty(nestKeys[i]);
            }
            catch (ConfigException e) {
                // if nest1.nest2 = 1 and nest1 = 1 are set together, this error happens.
                String nonObjectKey = Stream.of(Arrays.copyOfRange(nestKeys, 0, i + 1))
                    .collect(Collectors.joining("."));
                throw new ConfigException(String.format(ENGLISH,
                            "Parameter '%s' is set but '%s' is not a object (%s)",
                            key, nonObjectKey, nest.get(nestKeys[i], JsonNode.class)));
            }
        }
        nest.set(nestKeys[nestKeys.length - 1], value);
    }

    public static ProjectArchive loadProject(ProjectArchiveLoader projectLoader, String projectDirName, Config overrideParams)
        throws IOException
    {
        Path currentDirectory = Paths.get("").toAbsolutePath();
        Path projectPath;
        if (projectDirName == null) {
            projectPath = currentDirectory;
        }
        else {
            projectPath = Paths.get(projectDirName).normalize().toAbsolutePath();
        }

        // if projectPath is not current dir, set _project_path to overrideParams
        if (!projectPath.equals(currentDirectory)) {
            logger.info("Setting project path to {}", projectPath);
            overrideParams.set(LocalWorkspaceManager.PROJECT_PATH, projectPath.toString());
        }

        System.out.println("loadProject ーーーーーーーーーーーーーーー");
        System.out.println("ここは通っている");
        return projectLoader.load(projectPath, WorkflowResourceMatcher.defaultMatcher(), overrideParams);
    }

    public static String normalizeWorkflowName(ProjectArchive project, String workflowNameArg)
    {
        // if workflow argument ends with .dig, assume it is an OS-dependent path name and normalize it.
        // otherwise assume already normalized workflow name.
        if (workflowNameArg.endsWith(WORKFLOW_FILE_SUFFIX)) {
            String workflowResourceName = project.pathToResourceName(project.getProjectPath().resolve(workflowNameArg));
            return resourceNameToWorkflowName(workflowResourceName);
        }
        else {
            return workflowNameArg;
        }
    }
}
