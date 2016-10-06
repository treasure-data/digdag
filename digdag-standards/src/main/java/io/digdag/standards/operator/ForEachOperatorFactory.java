package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Limits;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ForEachOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(ForEachOperatorFactory.class);

    @Inject
    public ForEachOperatorFactory()
    { }

    public String getType()
    {
        return "for_each";
    }

    @Override
    public ForEachOperator newOperator(Path projectPath, TaskRequest request)
    {
        return new ForEachOperator(request);
    }

    static class ForEachOperator
            implements Operator
    {
        private final TaskRequest request;

        public ForEachOperator(TaskRequest request)
        {
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            Config map = params.parseNested("_command");

            LinkedHashMap<String, List<JsonNode>> entries = new LinkedHashMap<>();
            for (String key : map.getKeys()) {
                entries.put(key, map.parseList(key, JsonNode.class));
            }

            enforceTaskCountLimit(entries);

            List<Config> combinations = buildCombinations(request.getConfig().getFactory(), entries);

            boolean parallel = params.get("_parallel", boolean.class, false);

            Config generated = doConfig.getFactory().create();
            for (Config combination : combinations) {
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").setAll(combination);
                generated.set(
                        buildTaskName(combination),
                        subtask);
            }

            if (parallel) {
                generated.set("_parallel", parallel);
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }

        private static List<Config> buildCombinations(ConfigFactory cf, Map<String, List<JsonNode>> entries)
        {
            List<Config> current = new ArrayList<>();
            for (Map.Entry<String, List<JsonNode>> pair : entries.entrySet()) {
                List<Config> next = new ArrayList<>();
                if (current.isEmpty()) {
                    for (JsonNode value : pair.getValue()) {
                        next.add(cf.create().set(pair.getKey(), value));
                    }
                }
                else {
                    for (Config seed : current) {
                        for (JsonNode value : pair.getValue()) {
                            next.add(seed.deepCopy().set(pair.getKey(), value));
                        }
                    }
                }
                current = next;
            }
            return current;
        }

        private static void enforceTaskCountLimit(Map<String, List<JsonNode>> entries)
        {
            int count = 1;
            for (List<JsonNode> nodes : entries.values()) {
                count *= nodes.size();
                if (count > Limits.maxWorkflowTasks()) {
                    throw new ConfigException("Too many for_each subtasks. Limit: " + Limits.maxWorkflowTasks());
                }
            }
        }

        private static String buildTaskName(Config combination)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("+for-");
            boolean first = true;
            for (String key : combination.getKeys()) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append('&');
                }
                sb.append(key);
                sb.append('=');
                sb.append(encodeValue(combination, key));
            }
            return sb.toString();
        }

        private static String encodeValue(Config map, String key)
        {
            JsonNode node = map.get(key, JsonNode.class);
            String raw;
            if (node.isTextual()) {
                raw = node.textValue();
            }
            else {
                raw = node.toString();
            }
            try {
                return URLEncoder.encode(raw, "UTF-8").replace("+", "%20");  // "+" is not allowed as a task name
            }
            catch (UnsupportedEncodingException ex) {
                throw Throwables.propagate(ex);
            }
        }
    }
}
