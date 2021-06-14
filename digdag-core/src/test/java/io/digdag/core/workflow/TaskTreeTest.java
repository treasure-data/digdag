package io.digdag.core.workflow;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.Before;
import io.digdag.core.session.TaskRelation;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TaskTreeTest
{
    private TaskTree tree;

    @Before
    public void makeTree()
    {
        //                   1
        //                /      \
        //             2            3
        //         /  / \  \    /  /  \  \
        //        4  5-->6->7  8  9-->10->11
        //        |  |   |  |   / |   | \
        //       12 13  14 15 16 17  18->19
        //                        |
        //                       20
        tree = new TaskTree(ImmutableList.of(
                    root(1),

                    relation(1, 2),
                    relation(1, 3),

                    relation(2, 4),
                    relation(2, 5),
                    relation(2, 6, 5),
                    relation(2, 7, 6, 5),

                    relation(3, 8),
                    relation(3, 9),
                    relation(3, 10, 9),
                    relation(3, 11, 10),

                    relation(4, 12),
                    relation(5, 13),
                    relation(6, 14),
                    relation(7, 15),
                    relation(9, 16),
                    relation(9, 17),
                    relation(10, 18),
                    relation(10, 19, 18),

                    relation(17, 20)
                    ));
    }

    @Test
    public void testRootId()
            throws Exception
    {
        assertThat(
                tree.getRootTaskId(),
                is(1L));
    }

    @Test
    public void testRecursiveParentIdList()
            throws Exception
    {
        assertThat(
                tree.getRecursiveParentIdListFromRoot(12),
                is(list(1, 2, 4)));
        assertThat(
                tree.getRecursiveParentIdListFromRoot(13),
                is(list(1, 2, 5)));
        assertThat(
                tree.getRecursiveParentIdListFromRoot(14),
                is(list(1, 2, 6)));
        assertThat(
                tree.getRecursiveParentIdListFromRoot(15),
                is(list(1, 2, 7)));
    }

    @Test
    public void testRecursiveChildrenIdList()
            throws Exception
    {
        assertThat(
                tree.getRecursiveChildrenIdList(12),
                is(list()));
        assertThat(
                tree.getRecursiveChildrenIdList(4),
                is(list(12)));
        assertThat(
                tree.getRecursiveChildrenIdList(2),
                is(list(4, 12, 5, 13, 6, 14, 7, 15)));
        assertThat(
                tree.getRecursiveChildrenIdList(5),
                is(list(13)));
        assertThat(
                tree.getRecursiveChildrenIdList(6),
                is(list(14)));
    }

    @Test
    public void testRecursiveParentsUpstreamChildrenIdList()
            throws Exception
    {
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(1),
                is(list()));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(2),
                is(list(1)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(3),
                is(list(1)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(4),
                is(list(1, 2)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(5),
                is(list(1, 2)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(6),
                is(list(1, 2, 5, 13)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(7),
                is(list(1, 2, 5, 13, 6, 14)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(14),
                is(list(1, 2, 5, 13, 6)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(18),
                is(list(1, 3, 9, 16, 17, 20, 10)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(19),
                is(list(1, 3, 9, 16, 17, 20, 10, 18)));
        assertThat(
                tree.getRecursiveParentsUpstreamChildrenIdListFromFar(11),
                is(list(1, 3, 9, 16, 17, 20, 10, 18, 19)));
    }

    private static TaskRelation root(long id)
    {
        return TaskRelation.ofRoot(id);
    }

    private static TaskRelation relation(long parent, long id, long... upstreams)
    {
        return TaskRelation.of(id, parent, list(upstreams));
    }

    private static List<Long> list(long... prims)
    {
        ImmutableList.Builder<Long> b = ImmutableList.builder();
        for (long prim : prims) {
            b.add(prim);
        }
        return b.build();
    }
}
