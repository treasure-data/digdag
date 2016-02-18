package io.digdag.core.workflow;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.session.TaskRelation;

public class TaskTree
{
    // TODO This class includes unnecessary complexity. Node class is unnecessary.
    //      if walk methods lookup Map<Long, TaskRelation> when necessary

    public static class Node
    {
        private final TaskRelation relation;
        private Optional<Node> parent;
        private List<Node> children = new ArrayList<>();
        private List<Node> upstreams = new ArrayList<>();

        private Node(TaskRelation relation)
        {
            this.relation = relation;
        }

        public long getId()
        {
            return relation.getId();
        }

        public Optional<Node> getParent()
        {
            return parent;
        }

        public List<Node> getChildren()
        {
            return children;
        }

        public List<Node> getUpstreams()
        {
            return upstreams;
        }

        private TaskRelation getRelation()
        {
            return relation;
        }

        private void setParent(Optional<Node> parent)
        {
            this.parent = parent;
        }

        private void addChild(Node child)
        {
            this.children.add(child);
        }

        private void addUpstream(Node upstream)
        {
            this.upstreams.add(upstream);
        }

        public <T> T walkParentsRecursively(T value, Walker<T> walker)
        {
            if (!parent.isPresent()) {
                return value;
            }
            else {
                value = walker.walk(value, parent.get());
                return parent.get().walkParentsRecursively(value, walker);
            }
        }

        public <T> T walkChildrenRecursively(T value, Walker<T> walker)
        {
            for (Node child : children) {
                value = walker.walk(value, child);
                value = child.walkChildrenRecursively(value, walker);
            }
            return value;
        }

        public <T> T walkUpstreamSiblings(T value, Walker<T> walker)
        {
            return walkUpstreamSiblings(value, walker, new HashSet<>());
        }

        private <T> T walkUpstreamSiblings(T value, Walker<T> walker, Set<Long> walkedSet)
        {
            for (Node up : upstreams) {
                if (!walkedSet.contains(up.getId())) {
                    walkedSet.add(up.getId());
                    value = walker.walk(value, up);
                    value = up.walkUpstreamSiblings(value, walker, walkedSet);
                }
            }
            return value;
        }
    }

    public interface Walker <T>
    {
        T walk(T value, Node node);
    }

    private static Map<Long, Node> buildTree(List<TaskRelation> rels)
    {
        Map<Long, Node> map = new HashMap<>();
        for (TaskRelation rel : rels) {
            map.put(rel.getId(), new Node(rel));
        }

        // set parent and children
        for (Node node : map.values()) {
            Optional<Long> parentId = node.getRelation().getParentId();
            if (parentId.isPresent()) {
                Node parent = map.get(parentId.get());
                node.setParent(Optional.of(parent));
                parent.addChild(node);
            }
            else {
                node.setParent(Optional.absent());
            }
        }

        // set upstreams
        for (Node node : map.values()) {
            for (long id : node.getRelation().getUpstreams()) {
                Node upstream = map.get(id);
                node.addUpstream(upstream);
            }
        }

        return map;
    }

    private final Map<Long, Node> map;

    public TaskTree(List<TaskRelation> rels)
    {
        this.map = buildTree(rels);
    }

    public Node getNode(long id)
    {
        return Preconditions.checkNotNull(map.get(id));
    }

    public List<Long> getRecursiveParentIdList(long id)
    {
        return getNode(id).walkParentsRecursively(
                ImmutableList.<Long>builder(),
                (builder, parent) -> builder.add(parent.getId())).build();
    }

    public List<Long> getRecursiveParentsUpstreamChildrenIdList(long id)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        ImmutableList.copyOf(Iterables.concat(
                ImmutableList.of(id),
                getRecursiveParentIdList(id)))
            .stream()
            .forEach(parentId -> {
                builder.add(parentId);
                getNode(parentId).walkUpstreamSiblings(
                        builder,
                        (sameBuilder1, sib) -> {
                            sameBuilder1.add(sib.getId());
                            return sib.walkChildrenRecursively(sameBuilder1, (sameBuilder2, child) -> sameBuilder2.add(child.getId()));
                        });
            });
        return builder.build();
    }

    public List<Long> getRecursiveChildrenIdList(long id)
    {
        return getNode(id).walkChildrenRecursively(
                ImmutableList.<Long>builder(),
                (builder, child) -> builder.add(child.getId())).build();
    }
}
