package io.digdag.core.workflow;

import com.google.common.base.Optional;
import io.digdag.executor.DigdagEmbed;
import io.digdag.executor.LocalSite;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TaskDependenciesTest {

    private static DigdagEmbed embed;

    @BeforeClass
    public static void createDigdagEmbed()
            throws Exception {
        embed = WorkflowTestingUtils.setupEmbed();
    }

    @AfterClass
    public static void destroyDigdagEmbed()
            throws Exception {
        embed.close();
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LocalSite localSite;
    private Path projectPath;
    private TransactionManager tm;

    @Before
    public void setUp()
            throws Exception {
        this.localSite = embed.getLocalSite();
        this.tm = embed.getTransactionManager();
        this.projectPath = folder.newFolder().toPath();
    }

    @Test
    public void testTaskDependencies()
            throws Exception {
        StoredSessionAttemptWithSession attempt =
                runWorkflow(embed, projectPath, "basic", loadYamlResource("/io/digdag/core/workflow/basic.dig"));
        // # basic.dig
        // +step1:
        //  _type: noop
        //
        //+step2:
        //  _export:
        //    oval: o
        //    pval: p
        //
        //  +step3:
        //    _type: n${oval}${oval}${pval}
        //    _check:
        //      +step4:
        //        _type: noop
        //
        // id | tasks index  | full_name                   | expected parent_id | expected upstream_id 　
        // 1  | tasks.get(0) | +basic                      |                    |
        // 2  | tasks.get(1) | +basic+step1                | 1                  |
        // 3  | tasks.get(2) | +basic+step2                | 1                  | 2
        // 4  | tasks.get(3) | +basic+step2+step3          | 3                  |
        // 5  | tasks.get(4) | +basic+step2+step3+step4    | 4                  |

        tm.begin(() -> {
            assertThat(attempt.getStateFlags().isSuccess(), is(true));
            List<ArchivedTask> tasks = localSite.getSessionStore().getTasksOfAttempt(attempt.getId());

            // parent_id test
            assertThat(tasks.get(0).getParentId(), is(Optional.absent()));
            assertThat(tasks.get(1).getParentId(), is(Optional.of(tasks.get(0).getId())));
            assertThat(tasks.get(2).getParentId(), is(Optional.of(tasks.get(0).getId())));
            assertThat(tasks.get(3).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(4).getParentId(), is(Optional.of(tasks.get(3).getId())));

            // upstream_id test
            assertThat(tasks.get(0).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(1).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(2).getUpstreams(), is(Collections.singletonList(tasks.get(1).getId())));
            assertThat(tasks.get(3).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(4).getUpstreams(), is(Collections.emptyList()));

            return null;
        });
    }

    @Test
    public void testGroupRetryTaskDependencies()
            throws Exception {
        StoredSessionAttemptWithSession attempt =
                runWorkflow(embed, projectPath, "retry_on_group", loadYamlResource("/io/digdag/core/workflow/retry_on_group.dig"));
        tm.begin(() -> {
            assertThat(attempt.getStateFlags().isSuccess(), is(false));
            List<ArchivedTask> tasks = localSite.getSessionStore().getTasksOfAttempt(attempt.getId());
            // # retry_on_group.dig
            // +first:
            //   echo>: ""
            //   append_file: out
            // +doit:
            //   _retry: 3
            //   +task1:
            //     echo>: "try1"
            //     append_file: out
            //   +task2:
            //     +task2_nested:
            //       echo>: "try2"
            //       append_file: out
            //   +task3:
            //     fail>: task failed expectedly
            //   +task4:
            //     echo>: "skipped"
            //     append_file: out
            //
            // id | tasks index   | full_name                                | expected parent_id | expected upstream_id 　
            // 1  | tasks.get(0)  | +retry_on_group                          |                    |
            // 2  | tasks.get(1)  | +retry_on_group+first                    | 1                  |
            // 3  | tasks.get(2)  | +retry_on_group+doit                     | 1                  | 2
            // 4  | tasks.get(3)  | +retry_on_group+doit+task1               | 3                  |
            // 5  | tasks.get(4)  | +retry_on_group+doit+task2               | 3                  | 4
            // 6  | tasks.get(5)  | +retry_on_group+doit+task2+task2_nested  | 5                  |
            // 7  | tasks.get(6)  | +retry_on_group+doit+task3               | 3                  | 5
            // 8  | tasks.get(7)  | +retry_on_group+doit+task4               | 3                  | 7
            // 9  | tasks.get(8)  | +retry_on_group+doit+task1               | 3                  |
            // 10 | tasks.get(9)  | +retry_on_group+doit+task2               | 3                  | 9
            // 11 | tasks.get(10) | +retry_on_group+doit+task2+task2_nested  | 10                 |
            // 12 | tasks.get(11) | +retry_on_group+doit+task3               | 3                  | 10
            // 13 | tasks.get(12) | +retry_on_group+doit+task4               | 3                  | 12
            // 14 | tasks.get(13) | +retry_on_group+doit+task1               | 3                  |
            // 15 | tasks.get(14) | +retry_on_group+doit+task2               | 3                  | 14
            // 16 | tasks.get(15) | +retry_on_group+doit+task2+task2_nested  | 15                 |
            // 17 | tasks.get(16) | +retry_on_group+doit+task3               | 3                  | 15
            // 18 | tasks.get(17) | +retry_on_group+doit+task4               | 3                  | 17
            // 19 | tasks.get(18) | +retry_on_group+doit+task1               | 3                  |
            // 20 | tasks.get(19) | +retry_on_group+doit+task2               | 3                  | 19
            // 21 | tasks.get(20) | +retry_on_group+doit+task2+task2_nested  | 20                 |
            // 22 | tasks.get(21) | +retry_on_group+doit+task3               | 3                  | 20
            // 23 | tasks.get(22) | +retry_on_group+doit+task4               | 3                  | 22
            // 24 | tasks.get(23) | +retry_on_group^failure-alert            | 1                  |

            // parent_id test
            assertThat(tasks.get(0).getParentId(), is(Optional.absent()));
            assertThat(tasks.get(1).getParentId(), is(Optional.of(tasks.get(0).getId())));
            assertThat(tasks.get(2).getParentId(), is(Optional.of(tasks.get(0).getId())));
            assertThat(tasks.get(3).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(4).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(5).getParentId(), is(Optional.of(tasks.get(4).getId())));
            assertThat(tasks.get(6).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(7).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(8).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(9).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(10).getParentId(), is(Optional.of(tasks.get(9).getId())));
            assertThat(tasks.get(11).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(12).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(13).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(14).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(15).getParentId(), is(Optional.of(tasks.get(14).getId())));
            assertThat(tasks.get(16).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(17).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(18).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(19).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(20).getParentId(), is(Optional.of(tasks.get(19).getId())));
            assertThat(tasks.get(21).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(22).getParentId(), is(Optional.of(tasks.get(2).getId())));
            assertThat(tasks.get(23).getParentId(), is(Optional.of(tasks.get(0).getId())));

            // upstream_id test
            assertThat(tasks.get(0).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(1).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(2).getUpstreams(), is(Collections.singletonList(tasks.get(1).getId())));
            assertThat(tasks.get(3).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(4).getUpstreams(), is(Collections.singletonList(tasks.get(3).getId())));
            assertThat(tasks.get(5).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(6).getUpstreams(), is(Collections.singletonList(tasks.get(4).getId())));
            assertThat(tasks.get(7).getUpstreams(), is(Collections.singletonList(tasks.get(6).getId())));
            assertThat(tasks.get(8).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(9).getUpstreams(), is(Collections.singletonList(tasks.get(8).getId())));
            assertThat(tasks.get(10).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(11).getUpstreams(), is(Collections.singletonList(tasks.get(9).getId())));
            assertThat(tasks.get(12).getUpstreams(), is(Collections.singletonList(tasks.get(11).getId())));
            assertThat(tasks.get(13).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(14).getUpstreams(), is(Collections.singletonList(tasks.get(13).getId())));
            assertThat(tasks.get(15).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(16).getUpstreams(), is(Collections.singletonList(tasks.get(14).getId())));
            assertThat(tasks.get(17).getUpstreams(), is(Collections.singletonList(tasks.get(16).getId())));
            assertThat(tasks.get(18).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(19).getUpstreams(), is(Collections.singletonList(tasks.get(18).getId())));
            assertThat(tasks.get(20).getUpstreams(), is(Collections.emptyList()));
            assertThat(tasks.get(21).getUpstreams(), is(Collections.singletonList(tasks.get(19).getId())));
            assertThat(tasks.get(22).getUpstreams(), is(Collections.singletonList(tasks.get(21).getId())));
            assertThat(tasks.get(23).getUpstreams(), is(Collections.emptyList()));
            return null;
        });
    }
}
