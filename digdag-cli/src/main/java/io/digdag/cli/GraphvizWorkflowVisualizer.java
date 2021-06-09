package io.digdag.cli;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import io.digdag.commons.guava.ThrowablesUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

public class GraphvizWorkflowVisualizer
{
    private List<String> commands;

    public GraphvizWorkflowVisualizer()
    {
        this.commands = ImmutableList.of("dot", "-Tpng");
    }

    public void visualize(List<WorkflowVisualizerNode> nodes, File output)
            throws InterruptedException
    {
        Map<Long, NodeBuilder> idMap = new HashMap<>();
        for (WorkflowVisualizerNode node : nodes) {
            idMap.put(node.getId(), new NodeBuilder(node));
        }

        NodeBuilder root = null;
        for (NodeBuilder builder : idMap.values()) {
            if (builder.isRoot()) {
                root = builder;
            }
            else {
                NodeBuilder parent = idMap.get(builder.getParentId());
                if (parent == null) {
                    throw new IllegalArgumentException();
                }
                parent.addChild(builder);
            }
            builder.addUpstream(idMap);
        }
        if (root == null) {
            throw new IllegalArgumentException();
        }

        try {
            StringWriter writer = new StringWriter();
            try (PrintWriter out = new PrintWriter(writer)) {
                out.println("digraph session {");
                out.println("compound=true");
                root.buildTo(out);
                out.println("}");
            }
            String dot = writer.toString();

            generate(dot, output);
        }
        catch (IOException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    private void generate(String dot, File output)
            throws InterruptedException, IOException
    {
        List<String> c = new ArrayList<>(commands);
        c.add("-o");
        c.add(output.getAbsolutePath());

        ProcessBuilder pb = new ProcessBuilder(c);
        pb.redirectErrorStream(true);

        final Process p = pb.start();

        ByteArrayOutputStream message = new ByteArrayOutputStream();
        Thread t = new Thread(() -> {
            try {
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, message);
                }
            }
            catch (RuntimeException | IOException ex) {
                // TODO uncaught exception
                throw ThrowablesUtil.propagate(ex);
            }
        });
        t.start();

        try (OutputStream stdin = p.getOutputStream()) {
            stdin.write(dot.getBytes());
        }

        int ecode = p.waitFor();
        t.join();

        if (ecode != 0) {
            throw new IllegalArgumentException(message.toString());
        }
    }

    private static class NodeBuilder
    {
        private final WorkflowVisualizerNode node;
        private final List<NodeBuilder> children;
        private final List<NodeBuilder> upstreams;

        private NodeBuilder(WorkflowVisualizerNode node)
        {
            this.node = node;
            this.children = new ArrayList<>();
            this.upstreams = new ArrayList<>();
        }

        private long getId()
        {
            return node.getId();
        }

        private void addChild(NodeBuilder child)
        {
            children.add(child);
        }

        private void addUpstream(Map<Long, NodeBuilder> idMap)
        {
            for (long upstreamId : node.getUpstreamIds()) {
                NodeBuilder upstream = idMap.get(upstreamId);
                if (upstream == null) {
                    throw new IllegalArgumentException();
                }
                upstreams.add(upstream);
            }
        }

        private boolean isRoot()
        {
            return !node.getParentId().isPresent();
        }

        private long getParentId()
        {
            return node.getParentId().get();
        }

        private void buildTo(PrintWriter out)
        {
            if (children.isEmpty()) {
                out.println(String.format("\"%s\" [label=\"%s\" style=filled fillcolor=%s]",
                            node.getId(), node.getName(), getColor()));
            }
            else {
                out.println(String.format("subgraph \"cluster_%s\" {",
                            node.getId()));
                out.println(String.format("label = \"%s\"",
                            node.getName()));
                out.println("style=filled");
                out.println(String.format("bgcolor=%s", getColor()));
                for (NodeBuilder child : children) {
                    child.buildTo(out);
                    for (NodeBuilder upstream : child.upstreams) {
                        String src, dst;
                        String lopt = null, ropt = null;
                        if (upstream.children.isEmpty()) {
                            src = Long.toString(upstream.getId());
                        }
                        else {
                            src = firstLeafNodeId(upstream);
                            lopt = String.format("ltail=\"cluster_%s\"", upstream.getId());
                        }
                        if (child.children.isEmpty()) {
                            dst = Long.toString(child.getId());
                        }
                        else {
                            dst = firstLeafNodeId(child);
                            ropt = String.format("lhead=\"cluster_%s\"", child.getId());
                        }

                        if (lopt == null && ropt == null) {
                            out.println(String.format("\"%s\" -> \"%s\"",
                                        src, dst));
                        }
                        else {
                            out.println(String.format("\"%s\" -> \"%s\" [%s]",
                                        src, dst,
                                        Stream.of(lopt, ropt)
                                            .filter(it -> it != null)
                                            .collect(Collectors.joining(","))
                                        ));
                        }
                    }
                }
                out.println("}");
            }
        }

        private String getColor()
        {
            switch (node.getState()) {
            case BLOCKED:
                return "gray74";
            case READY:
                return "gray80";
            case RETRY_WAITING:
                return "gray94";
            case GROUP_RETRY_WAITING:
                return "gray94";
            case RUNNING:
                return "yellowgreen";
            case PLANNED:
                return "slateblue1";
            case GROUP_ERROR:
                return "yellow";
            case SUCCESS:
                return "olivedrab3";
            case ERROR:
                return "indianred1";
            case CANCELED:
                return "graph50";
            default:
                throw new IllegalStateException("Unknown task status code");
            }
        }

        private static String firstLeafNodeId(NodeBuilder builder)
        {
            if (builder.children.isEmpty()) {
                return Long.toString(builder.getId());
            }
            else {
                return firstLeafNodeId(builder.children.get(0));
            }
        }
    }
}
