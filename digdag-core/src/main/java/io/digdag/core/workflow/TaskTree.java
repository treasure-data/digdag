package io.digdag.core.workflow;

import java.util.Objects;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.digdag.core.session.TaskRelation;

public class TaskTree
{
    public interface Walker <T>
    {
        T walk(T value, TaskRelation node);
    }

    private final Map<Long, TaskRelation> map;

    public TaskTree(List<TaskRelation> rels)
    {
        ImmutableMap.Builder<Long, TaskRelation> builder = ImmutableMap.builder();
        for (TaskRelation rel : rels) {
            builder.put(rel.getId(), rel);
        }
        this.map = builder.build();
    }

    public long getRootTaskId()
    {
        for (TaskRelation rel : map.values()) {
            if (!rel.getParentId().isPresent()) {
                return rel.getId();
            }
        }
        throw new IllegalStateException("Root task doesn't exist in an attempt: "+map.values());
    }

    private TaskRelation get(long id)
    {
        return Objects.requireNonNull(map.get(id));
    }

    private Optional<TaskRelation> getParent(long id)
    {
        return get(id).getParentId().transform(it -> get(it));
    }

    public List<Long> getRecursiveParentIdListFromRoot(long id)
    {
        return walkParentsRecursively(id,
                ImmutableList.<Long>builder(),
                (builder, parent) -> builder.add(parent.getId())).build();
    }

    private <T> T walkParentsRecursively(long id, T value, Walker<T> walker)
    {
        Optional<TaskRelation> parent = getParent(id);
        if (!parent.isPresent()) {
            return value;
        }
        else {
            value = walkParentsRecursively(parent.get().getId(), value, walker);
            return walker.walk(value, parent.get());
        }
    }

    public List<Long> getRecursiveParentsUpstreamChildrenIdListFromFar(long id)
    {
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        ImmutableList.copyOf(Iterables.concat(
                getRecursiveParentIdListFromRoot(id),
                ImmutableList.of(id)))
            .stream()
            .forEach(parentId -> {
                walkUpstreamSiblings(parentId,
                        builder,
                        (sameBuilder1, sib) -> {
                            sameBuilder1.add(sib.getId());
                            walkChildrenRecursively(sib.getId(), sameBuilder1, (sameBuilder2, child) -> sameBuilder2.add(child.getId()));
                            return builder;
                        });
                if (parentId != id) {
                    // exclude given id itself
                    builder.add(parentId);
                }
            });
        return builder.build();
    }

    public <T> T walkChildrenRecursively(long id, T value, Walker<T> walker)
    {
        for (TaskRelation rel : map.values()) {
            if (rel.getParentId().isPresent() && rel.getParentId().get() == id) {
                TaskRelation child = rel;
                value = walker.walk(value, child);
                value = walkChildrenRecursively(child.getId(), value, walker);
            }
        }
        return value;
    }

    private <T> T walkUpstreamSiblings(long id, T value, Walker<T> walker)
    {
        return walkUpstreamSiblings(id, value, walker, new HashSet<>());
    }

    private <T> T walkUpstreamSiblings(long id, T value, Walker<T> walker, Set<Long> walkedSet)
    {
        Set<Long> upstreams = ImmutableSet.copyOf(get(id).getUpstreams());
        for (TaskRelation rel : map.values()) {
            // here uses order of map.values instead of order of get(id).getUpstreams
            // so that farther (younger) siblings always comes first
            if (upstreams.contains(rel.getId())) {
                // here has deduplication because upstream ids could include
                // same id with with upstream's upstreams.
                if (walkedSet.add(rel.getId())) {
                    value = walkUpstreamSiblings(rel.getId(), value, walker, walkedSet);
                    value = walker.walk(value, rel);
                }
            }
        }
        return value;
    }

    public List<Long> getRecursiveChildrenIdList(long id)
    {
        return walkChildrenRecursively(id,
                ImmutableList.<Long>builder(),
                (builder, child) -> builder.add(child.getId())).build();
    }
}
