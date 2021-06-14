package acceptance;

import io.digdag.client.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.LocalVersion;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;

import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class VersionIT
{
    private static final Version REMOTE_VERSION = Version.parse("4.5.6");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder()
        .version(REMOTE_VERSION)
        .configuration("server.client-version-check.upgrade-recommended-if-older = 4.5.0")
        .configuration("server.client-version-check.api-incompatible-if-older = 4.0.0")
        .build();

    private Path config;

    @Before
    public void setUp()
            throws Exception
    {
        config = Files.createFile(folder.getRoot().toPath().resolve("config"));
    }

    @Test
    public void testVersionHeader()
            throws Exception
    {
        String versionString = "3.14.15";
        CommandStatus status = main(LocalVersion.of(Version.parse(versionString)), "-c", config.toString());
        assertThat(status.errUtf8(), containsString("Digdag v" + versionString));
    }

    @Test
    public void testVersionCheckWithCompatibleVersion()
            throws Exception
    {
        Version localVersion = Version.parse("4.5.0");

        // interactive succeeds
        CommandStatus interactive = main(LocalVersion.of(localVersion).withBatchModeCheck(false), "workflows", "-e", server.endpoint(), "-c", config.toString());
        assertThat(interactive.code(), is(0));  // success

        // batch succeeds
        CommandStatus batch = main(LocalVersion.of(localVersion).withBatchModeCheck(true), "workflows", "-e", server.endpoint(), "-c", config.toString());
        assertThat(batch.code(), is(0));  // success
    }

    @Test
    public void testVersionCheckWithUpgradeRecommendedVersion()
            throws Exception
    {
        Version localVersion = Version.parse("4.4.3");

        // interactive mode aborts
        CommandStatus interactive = main(LocalVersion.of(localVersion).withBatchModeCheck(false), "workflows", "-e", server.endpoint(), "-c", config.toString());
        assertThat(interactive.code(), is(not(0)));  // fail
        assertThat(interactive.errUtf8(), containsString("client: " + localVersion));
        assertThat(interactive.errUtf8(), containsString("server: " + REMOTE_VERSION));
        assertThat(interactive.errUtf8(), containsString(
                    "This client version is obsoleted. It is recommended to upgrade"));
        assertThat(interactive.errUtf8(), containsString(
                    "Please run following command locally to upgrade to the latest version compatible to the server:"));
        assertThat(interactive.errUtf8(), containsString("digdag selfupdate "  + REMOTE_VERSION));

        // batch mode warns
        CommandStatus batch = main(LocalVersion.of(localVersion).withBatchModeCheck(true), "workflows", "-e", server.endpoint(), "-c", config.toString());
        assertThat(batch.code(), is(0));  // success
    }

    @Test
    public void testVersionCheckWithApiIncompatibleVersion()
            throws Exception
    {
        Version localVersion = Version.parse("3.9");

        // interactive mode aborts
        CommandStatus interactive = main(LocalVersion.of(localVersion).withBatchModeCheck(false), "workflows", "-e", server.endpoint(), "-c", config.toString());
        assertThat(interactive.code(), is(not(0)));  // fail
        assertThat(interactive.errUtf8(), containsString("client: " + localVersion));
        assertThat(interactive.errUtf8(), containsString("server: " + REMOTE_VERSION));
        assertThat(interactive.errUtf8(), containsString(
                    "This client version is not API compatible to the server"));
        assertThat(interactive.errUtf8(), containsString(
                    "Please run following command locally to upgrade to a compatible version with the server:"));
        assertThat(interactive.errUtf8(), containsString("digdag selfupdate "  + REMOTE_VERSION));

        // batch mode aborts
        CommandStatus batch = main(LocalVersion.of(localVersion).withBatchModeCheck(true), "workflows", "-e", server.endpoint(), "-c", config.toString());
        assertThat(batch.code(), is(not(0)));  // fail
        assertThat(batch.errUtf8(), containsString("client: " + localVersion));
        assertThat(batch.errUtf8(), containsString("server: " + REMOTE_VERSION));
        assertThat(batch.errUtf8(), containsString(
                    "This client version is not API compatible to the server"));
        assertThat(batch.errUtf8(), containsString(
                    "Please run following command locally to upgrade to a compatible version with the server:"));
        assertThat(batch.errUtf8(), containsString("digdag selfupdate "  + REMOTE_VERSION));
    }
}
