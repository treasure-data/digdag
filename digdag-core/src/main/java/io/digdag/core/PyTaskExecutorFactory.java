package io.digdag.core;

import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.io.OutputStream;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.config.Config;
import io.digdag.core.config.MutableConfig;

public class PyTaskExecutorFactory
        implements TaskExecutorFactory
{
    private static Logger logger = LoggerFactory.getLogger(PyTaskExecutorFactory.class);

    private final ObjectMapper mapper;

    @Inject
    public PyTaskExecutorFactory(ObjectMapper mapper)
    {
        this.mapper = mapper;
    }

    public String getType()
    {
        return "py";
    }

    public TaskExecutor newTaskExecutor(Config config, Config params, Config state)
    {
        return new PyTaskExecutor(config, params, state);
    }

    private class PyTaskExecutor
            extends BaseTaskExecutor
    {
        public PyTaskExecutor(Config config, Config params, Config state)
        {
            super(config, params, state);
        }

        @Override
        public Config runTask(Config config, Config params)
        {
            Config data;
            try {
                data = runCode(config, params, "run");
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            subtaskConfig.setAll(data.getNestedOrGetEmpty("sub"));
            inputs.addAll(data.getListOrEmpty("inputs", Config.class));
            outputs.addAll(data.getListOrEmpty("outputs", Config.class));
            return data.getNestedOrGetEmpty("carry_params");
        }

        private Config runCode(Config c, Config params, String methodName)
                throws IOException, InterruptedException
        {
            MutableConfig config = c.mutable();
            File inFile = File.createTempFile("digdag-py-in-", ".tmp");  // TODO use TempFileAllocator
            File outFile = File.createTempFile("digdag-py-out-", ".tmp");  // TODO use TempFileAllocator

            if (config.has("command")) {
                String command = config.get("command", String.class);
                String[] fragments = command.split("\\.");
                String klass = fragments[fragments.length - 1];

                StringBuilder sb = new StringBuilder();
                if (fragments.length > 1) {
                    String pkg = Arrays.asList(fragments).subList(0, fragments.length-1)
                        .stream()
                        .collect(Collectors.joining("."));
                    sb.append("from ")
                        .append(pkg)
                        .append(" import ")
                        .append(klass)
                        .append("\n");
                }
                sb.append("task = ").append(klass).append("(config, state, params)").append("\n");
                sb.append("task.").append(methodName).append("()\n\n");

                sb.append("out = dict()\n");
                sb.append("if hasattr(task, 'sub'):\n");
                sb.append("    out['sub'] = task.sub\n");
                sb.append("if hasattr(task, 'carry_params'):\n");
                sb.append("    out['carry_params'] = task.carry_params\n");
                sb.append("if hasattr(task, 'inputs'):\n");
                sb.append("    out['inputs'] = task.inputs  # TODO check callable\n");
                sb.append("if hasattr(task, 'outputs'):\n");
                sb.append("    out['outputs'] = task.outputs  # TODO check callable\n");
                sb.append("with open(out_file, 'w') as out_file:\n");
                sb.append("    json.dump(out, out_file)\n");

                config.set("script", sb.toString());
            }

            String script = config.get("script", String.class);

            StringBuilder sb = new StringBuilder();
            sb.append("import json\n");
            sb.append("import sys\n");
            sb.append("import os\n");
            sb.append("in_file = \"").append(inFile.getPath()).append("\"\n");
            sb.append("out_file = \"").append(outFile.getPath()).append("\"\n");
            sb.append("sys.path.append(os.path.abspath(os.getcwd()))\n");
            sb.append("with open(in_file) as f:\n");
            sb.append("    in_data = json.load(f)\n");
            sb.append("    config = in_data['config']\n");
            sb.append("    params = in_data['params']\n");
            sb.append("    state = in_data['state']\n");
            sb.append("\n");
            sb.append(script);

            String code = sb.toString();

            logger.info("py>: {}", config.get("command", String.class, script));
            logger.trace("Python code ===\n{}\n", code);

            try (FileOutputStream fo = new FileOutputStream(inFile)) {
                mapper.writeValue(fo, ImmutableMap.of(
                            "config", config,
                            "params", params,
                            "state", state));
            }

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                ProcessBuilder pb = new ProcessBuilder("python", "-");
                pb.redirectErrorStream(true);
                Process p = pb.start();
                try (Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
                    writer.write(code);
                }
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, buffer);
                }
                ecode = p.waitFor();
                message = buffer.toString();
            }

            //logger.info("Python message ===\n{}", message);  // TODO include task name
            System.out.println(message);
            if (ecode != 0) {
                throw new RuntimeException("Python command failed: "+message);
            }

            return mapper.readValue(outFile, Config.class);
        }
    }
}
