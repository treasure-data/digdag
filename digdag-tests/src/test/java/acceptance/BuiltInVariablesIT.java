package acceptance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static utils.TestUtils.validUuid;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class BuiltInVariablesIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void testRun()
            throws Exception
    {
        copyResource("acceptance/built_in_variables/built_in_variables.dig", root().resolve("built_in_variables.dig"));
        CommandStatus status = main("run",
                "--project", root().toString(),
                "-o", root().toString(),
                "built_in_variables.dig",
                "-t", "2016-01-02 03:04:05");
        assertThat(status.errUtf8(), status.code(), is(0));
        Path outputFile = root().resolve("output.yml");

        String output = new String(Files.readAllBytes(outputFile), UTF_8);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Map<String, String> values = mapper.readValue(output, new TypeReference<Map<String, String>>() {});

        System.out.println(values);

        Map<String, Matcher<String>> expectedOutput = ImmutableMap.<String, Matcher<String>>builder()
                .put("session_tz_offset", is("+0000"))
                .put("session_time", is("2016-01-02T03:04:05+00:00"))
                .put("session_id", is("1"))
                .put("session_uuid", is(validUuid()))
                .put("timezone", is("UTC"))
                .put("session_local_time", is("2016-01-02 03:04:05"))
                .put("last_session_date", is("2016-01-02"))
                .put("session_date_compact", is("20160102"))
                .put("last_session_time", is("2016-01-02T00:00:00+00:00"))
                .put("session_unixtime", is("1451703845"))
                .put("last_session_date_compact", is("20160102"))
                .put("session_date", is("2016-01-02"))
                .put("last_session_local_time", is("2016-01-02 00:00:00"))
                .put("last_session_tz_offset", is("+0000"))
                .put("next_session_date_compact", is("20160103"))
                .put("next_session_date", is("2016-01-03"))
                .put("last_session_unixtime", is("1451692800"))
                .put("next_session_time", is("2016-01-03T00:00:00+00:00"))
                .put("next_session_local_time", is("2016-01-03 00:00:00"))
                .put("next_session_tz_offset", is("+0000"))
                .put("next_session_unixtime", is("1451779200"))
                .put("task_name", is("+built_in_variables+get_variables+task_name"))
                .put("attempt_id", is("1"))
                .build();

        assertThat(values.entrySet().size(), is(expectedOutput.size()));

        expectedOutput.entrySet().stream().forEach(
                e -> assertThat(values.get(e.getKey()), e.getValue()));
    }
}
