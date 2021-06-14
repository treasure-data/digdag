package acceptance;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;

import static utils.TestUtils.copyResource;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConditionalOperatorIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void testMeta()
            throws Exception
    {
        copyResource("acceptance/conditional/conditional.dig", root().resolve("conditional.dig"));
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "conditional.dig", "-p", "n=3", "-p", "val=1");
        assertThat(Files.exists(root().resolve("loop_0.out")), is(true));
        assertThat(Files.exists(root().resolve("loop_1.out")), is(true));
        assertThat(Files.exists(root().resolve("loop_2.out")), is(true));
        assertThat(Files.exists(root().resolve("loop_3.out")), is(false));
        assertThat(Files.exists(root().resolve("eat_apple.out")), is(true));
        assertThat(Files.exists(root().resolve("throw_apple.out")), is(true));
        assertThat(Files.exists(root().resolve("eat_orange.out")), is(true));
        assertThat(Files.exists(root().resolve("throw_orange.out")), is(true));
        assertThat(Files.exists(root().resolve("if.out")), is(true));
        assertThat(Files.exists(root().resolve("else.out")), is(false));
    }

    @Test
    public void testConditionalFail()
            throws Exception
    {
        copyResource("acceptance/conditional/fail.dig", root().resolve("fail.dig"));

        // cond = fail
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "fail.dig", "-p", "cond=fail", "--rerun");
        assertThat(Files.exists(root().resolve("fail.out")), is(false));

        // cond = success
        TestUtils.main("run", "-o", root().toString(), "--project", root().toString(), "fail.dig", "-p", "cond=success", "--rerun");
        assertThat(Files.exists(root().resolve("success.out")), is(true));
    }
}
