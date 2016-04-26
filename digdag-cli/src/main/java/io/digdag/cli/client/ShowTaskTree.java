package io.digdag.cli.client;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import io.digdag.core.Version;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowTaskTree
        extends ClientCommand
{
    public ShowTaskTree(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
            throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        showTaskTree(parseLongOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag tasktree <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    public void showTaskTree(long attemptId)
            throws Exception
    {
        DigdagClient client = buildClient();
        RestSessionAttempt attempt = client.getSessionAttempt(attemptId);
        List<RestTask> tasks = client.getTasks(attemptId);

        List<TreeNode> roots = new ArrayList<>();
        Map<Long, TreeNode> forest = new HashMap<>();

        // Populate forest
        for (RestTask task : tasks) {
            TreeNode node = TreeNode.of(task);
            forest.put(task.getId(), node);
        }

        // Build trees
        for (RestTask task : tasks) {
            TreeNode node = forest.get(task.getId());
            Optional<Long> parentId = task.getParentId();
            if (!parentId.isPresent()) {
                roots.add(node);
            }
            else {
                TreeNode parent = forest.get(parentId.get());
                parent.children.put(task.getId(), node);
            }
        }

        // Print trees, omitting the roots
        for (TreeNode root : roots) {
            printTree(forest, root, 0);
        }

        ln("%d tasks.", tasks.size());
    }

    private void printTree(Map<Long, TreeNode> forest, TreeNode node, int level)
    {
        node.children.keySet().stream().sorted().forEach(
                id -> printTree(forest, forest.get(id), level, node.task.getFullName()));
    }

    private void printTree(Map<Long, TreeNode> forest, TreeNode node, int level, String namePrefix)
    {
        RestTask task = node.task;
        String name = task.getFullName().substring(namePrefix.length() + 1);
        ln(Strings.repeat(" ", level) + "+" + name + ": " + task.getState());
        printTree(forest, node, level + 1);
    }

    private static class TreeNode
    {
        private final long taskId;
        private final RestTask task;
        private final Map<Long, TreeNode> children = new HashMap<>();

        public TreeNode(long taskId, RestTask task)
        {
            this.taskId = taskId;
            this.task = task;
        }

        public static TreeNode of(RestTask task)
        {
            return new TreeNode(task.getId(), task);
        }
    }
}
