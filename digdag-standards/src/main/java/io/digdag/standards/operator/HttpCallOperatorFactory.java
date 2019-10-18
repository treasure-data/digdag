package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Environment;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowFile;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.util.UserSecretTemplate;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.digdag.util.Workspace.propagateIoException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

public class HttpCallOperatorFactory
        extends HttpOperatorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(HttpCallOperatorFactory.class);

    private static final String WORKFLOW_FILE_NAME = "http_call.dig";

    private final ConfigFactory cf;
    private final ObjectMapper mapper;
    private final YAMLFactory yaml;
    private final ProjectArchiveLoader projectLoader;
    private final int maxResponseContentSize;

    @Inject
    public HttpCallOperatorFactory(ConfigFactory cf,
            Config systemConfig, @Environment Map<String, String> env)
    {
        super(systemConfig, env);
        this.cf = cf;
        this.mapper = new ObjectMapper();
        this.yaml = new YAMLFactory()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false);
        this.projectLoader = new ProjectArchiveLoader(
                new ConfigLoaderManager(
                    cf,
                    new YamlConfigLoader()));
        this.maxResponseContentSize = systemConfig.get("config.http_call.max_response_content_size", int.class, 64 * 1024);
    }

    @Override
    public String getType()
    {
        return "http_call";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new HttpCallOperator(context);
    }

    class HttpCallOperator
            extends HttpOperator
    {
        public HttpCallOperator(OperatorContext context)
        {
            super(context);
        }

        @Override
        public TaskResult runTask()
        {
            UserSecretTemplate uriTemplate = UserSecretTemplate.of(params.get("_command", String.class));
            boolean uriIsSecret = uriTemplate.containsSecrets();
            URI uri = URI.create(uriTemplate.format(context.getSecrets()));
            String mediaTypeOverride = params.getOptional("content_type_override", String.class).orNull();

            ContentResponse response;

            HttpClient httpClient = client();
            try {
                response = runHttp(httpClient, uri, uriIsSecret);
            }
            finally {
                stop(httpClient);
            }

            String content;
            if (Strings.isNullOrEmpty(mediaTypeOverride)) {
                // This ContentResponse::getContentAsString considers ;charset= parameter
                // of Content-Type. If not set, it uses UTF-8.
                content = response.getContentAsString();
            }
            else {
                // This logic mimics how org.eclipse.jetty.client.HttpContentResponse::getContentAsString handles Content-Type
                int index = mediaTypeOverride.toLowerCase(ENGLISH).indexOf("charset=");
                if (index > 0) {
                    String encoding = mediaTypeOverride.substring(index + "charset=".length());
                    try {
                        content = new String(response.getContent(), encoding);
                    }
                    catch (UnsupportedEncodingException e) {
                        throw new UnsupportedCharsetException(encoding);
                    }
                }
                else {
                    content = new String(response.getContent(), UTF_8);
                }
            }

            // validate response length
            if (content.length() > maxResponseContentSize) {
                throw new TaskExecutionException("Response content too large: " + content.length() + " > " + maxResponseContentSize);
            }

            // parse content based on response media type
            String digFileSource = reformatDigFile(content, response.getMediaType(), mediaTypeOverride);
            // write to http_call.dig file
            Path workflowPath = writeDigFile(digFileSource);

            // following code is almost same with CallOperatorFactory.CallOperator.runTask
            Config config = request.getConfig();

            WorkflowFile workflowFile;
            try {
                workflowFile = projectLoader.loadWorkflowFileFromPath(
                        workspace.getProjectPath(), workflowPath, config.getFactory().create());
            }
            catch (IOException ex) {
                throw propagateIoException(ex, WORKFLOW_FILE_NAME, ConfigException::new);
            }

            Config def = workflowFile.toWorkflowDefinition().getConfig();

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(def)
                .build();
        }

        private String reformatDigFile(String content, String mediaTypeString, String mediaTypeOverride)
        {
            MediaType mediaType;
            if (Strings.isNullOrEmpty(mediaTypeOverride)) {
                if (Strings.isNullOrEmpty(mediaTypeString)) {
                    throw new TaskExecutionException("Content-Type must be set in the HTTP response but not set");
                }
                mediaType = MediaType.valueOf(mediaTypeString);
            }
            else {
                mediaType = MediaType.valueOf(mediaTypeOverride);
            }

            String t = mediaType.getType() + "/" + mediaType.getSubtype();  // without ;charset= or other params
            switch (t) {
            case MediaType.APPLICATION_JSON:
                try {
                    // parse as json
                    Config sourceConfig = cf.fromJsonString(content);
                    // reformat as yaml
                    return formatYaml(sourceConfig);
                }
                catch (ConfigException ex) {
                    throw new RuntimeException("Failed to parse response as JSON: " + ex.getMessage(), ex);
                }

            case "application/x-yaml":
                // use as-is; let projectLoader.loadWorkflowFileFromPath handle parse errors
                return content;

            //case MediaType.TEXT_PLAIN:
            //case MediaType.APPLICATION_OCTET_STREAM:

            default:
                throw new TaskExecutionException("Unsupported Content-Type (expected application/json or application/x-yaml): " + mediaTypeString);
            }
        }

        private String formatYaml(Config value)
        {
            try {
                StringWriter writer = new StringWriter();
                try (YAMLGenerator out = yaml.createGenerator(writer)) {
                    mapper.writeValue(out, value);
                }
                return writer.toString();
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private Path writeDigFile(String source)
        {
            Path workflowPath = workspace.getPath(WORKFLOW_FILE_NAME);
            try (BufferedWriter writer = Files.newBufferedWriter(workflowPath, UTF_8)) {
                writer.write(source);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return workflowPath;
        }
    }
}
