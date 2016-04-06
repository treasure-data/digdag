package io.digdag.core.workflow;

import java.util.Objects;
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

    public List<Long> getRecursiveParentIdList(long id)
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
            value = walker.walk(value, parent.get());
            return walkParentsRecursively(parent.get().getId(), value, walker);
        }
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
                walkUpstreamSiblings(id,
                        builder,
                        (sameBuilder1, sib) -> {
                            sameBuilder1.add(sib.getId());
                            return walkChildrenRecursively(sib.getId(), sameBuilder1, (sameBuilder2, child) -> sameBuilder2.add(child.getId()));
                        });
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
        for (long upId : get(id).getUpstreams()) {
            TaskRelation up = get(upId);
            if (!walkedSet.contains(up.getId())) {
                walkedSet.add(up.getId());
                value = walker.walk(value, up);
                value = walkUpstreamSiblings(up.getId(), value, walker, walkedSet);
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
