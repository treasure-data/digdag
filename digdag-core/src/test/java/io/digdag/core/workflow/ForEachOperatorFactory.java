package io.digdag.core.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.ParallelControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
    public ForEachOperator newOperator(OperatorContext context)
    {
        return new ForEachOperator(context);
    }

    static class ForEachOperator
            implements Operator
    {
        private final TaskRequest request;
        private final OperatorContext context;

        public ForEachOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
            this.context = context;
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            Config map = params.parseNested("_command");

            LinkedHashMap<String, List<JsonNode>> entries = new LinkedHashMap<>();
            for (String key : map.getKeys()) {
                entries.put(key, map.parseList(key, JsonNode.class));
            }

            enforceTaskCountLimit(entries);

            List<Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>>> combinations = buildCombinations(entries);

            Config generated = doConfig.getFactory().create();
            for (Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>> combination : combinations) {
                Config combinationConfig = params.getFactory().create();
                for (Map.Entry<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>> entry : combination.entrySet()) {
                    combinationConfig.set(entry.getKey().getValue(), entry.getValue().getValue());
                }
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").setAll(combinationConfig);
                generated.set(
                        buildTaskName(combination),
                        subtask);
            }

            ParallelControl.of(params).copyIfNeeded(generated);

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }

        private static List<Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>>> buildCombinations(Map<String, List<JsonNode>> entries)
        {
            List<Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>>> current = new ArrayList<>();
            ImmutableList<Map.Entry<String, List<JsonNode>>> entriesList = ImmutableList.copyOf(entries.entrySet());
            for (int i = 0; i < entriesList.size(); i++) {
                Map.Entry<String, List<JsonNode>> pair = entriesList.get(i);
                List<Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>>> next = new ArrayList<>();
                List<JsonNode> items = pair.getValue();
                if (current.isEmpty()) {
                    for (int j = 0; j < items.size(); j++) {
                        Map.Entry<Integer, String> key = Maps.immutableEntry(i, pair.getKey());
                        Map.Entry<Integer, JsonNode> value = Maps.immutableEntry(j, items.get(j));
                        next.add(ImmutableMap.of(key, value));
                    }
                }
                else {
                    for (Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>> seed : current) {
                        for (int j = 0; j < items.size(); j++) {
                            Map.Entry<Integer, String> key = Maps.immutableEntry(i, pair.getKey());
                            Map.Entry<Integer, JsonNode> value = Maps.immutableEntry(j, items.get(j));
                            next.add(ImmutableMap.<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>>builder()
                                    .putAll(seed)
                                    .put(key, value)
                                    .build());
                        }
                    }
                }
                current = next;
            }
            return current;
        }

        private void enforceTaskCountLimit(Map<String, List<JsonNode>> entries)
        {
            int count = 1;
            for (List<JsonNode> nodes : entries.values()) {
                count *= nodes.size();
                if (count > context.getMaxWorkflowTasks()) {
                    throw new ConfigException("Too many for_each subtasks. Limit: " + context.getMaxWorkflowTasks());
                }
            }
        }

        private static String buildTaskName(Map<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>> combination)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("+for-");
            boolean first = true;
            for (Map.Entry<Map.Entry<Integer, String>, Map.Entry<Integer, JsonNode>> entry : combination.entrySet()) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append('&');
                }
                Map.Entry<Integer, String> key = entry.getKey();
                sb.append(key.getKey()).append('=').append(nameTag(key.getValue()));
                sb.append('=');
                Map.Entry<Integer, JsonNode> value = entry.getValue();
                sb.append(value.getKey()).append('=').append(nameTag(value.getValue()));
            }
            return sb.toString();
        }

        private static String nameTag(JsonNode node)
        {
            String raw;
            if (node.isTextual()) {
                raw = node.textValue();
            }
            else {
                raw = node.toString();
            }
            return nameTag(raw);
        }

        private static String nameTag(String s)
        {
            if (s.length() > 8) {
                s = s.substring(0, 8);
            }
            try {
                return URLEncoder.encode(s, "UTF-8").replace("+", "%20");  // "+" is not allowed as a task name
            }
            catch (UnsupportedEncodingException ex) {
                throw ThrowablesUtil.propagate(ex);
            }
        }
    }
}
