package io.digdag.client.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static io.digdag.client.config.ConfigUtils.newConfig;

public class ConfigTest
{
    private Config config;

    @Before
    public void setUp()
    {
        config = newConfig();
    }

    @Test
    public void testSetGetPrimitives()
    {
        config.set("int", 1);
        config.set("str", "s");
        config.set("bool", true);

        assertThat(config.get("int", byte.class), is((byte) 1));
        assertThat(config.get("int", short.class), is((short) 1));
        assertThat(config.get("int", int.class), is(1));
        assertThat(config.get("int", long.class), is(1L));

        assertThat(config.get("int", Byte.class), is((byte) 1));
        assertThat(config.get("int", Short.class), is((short) 1));
        assertThat(config.get("int", Integer.class), is(1));
        assertThat(config.get("int", Long.class), is(1L));

        assertThat(config.get("str", String.class), is("s"));

        assertThat(config.get("bool", boolean.class), is(true));
        assertThat(config.get("bool", Boolean.class), is(true));
    }

    @Test
    public void verifyAutoStringConvert()
    {
        config.set("int", 1);
        config.set("float", 0.2);
        config.set("bool", true);

        assertThat(config.get("int", String.class), is("1"));
        assertThat(config.get("float", String.class), is("0.2"));
        assertThat(config.get("bool", String.class), is("true"));
    }

    @Test
    public void verifySetGetNested()
    {
        Config nested = newConfig().set("k", newConfig());
        config.set("nested", nested);
        assertThat(config.deepCopy().getNested("nested"), is(nested));
        assertThat(config.deepCopy().getNestedOrGetEmpty("nested"), is(nested));
        assertThat(config.deepCopy().getNestedOrSetEmpty("nested"), is(nested));
        assertThat(config.deepCopy().getOptionalNested("nested"), is(Optional.of(nested)));
        assertThat(config.deepCopy().parseNested("nested"), is(nested));
        assertThat(config.deepCopy().parseNestedOrGetEmpty("nested"), is(nested));
    }

    @Test
    public void verifySetGetNestedNotSet()
    {
        Config empty = newConfig();
        assertConfigException(() -> config.deepCopy().getNested("nested"));
        assertThat(config.deepCopy().getNestedOrGetEmpty("nested"), is(empty));
        assertThat(config.deepCopy().getNestedOrSetEmpty("nested"), is(empty));
        assertThat(config.deepCopy().getOptionalNested("nested"), is(Optional.absent()));
        assertConfigException(() -> config.deepCopy().parseNested("nested"));
        assertThat(config.deepCopy().parseNestedOrGetEmpty("nested"), is(empty));
    }

    @Test
    public void verifyParseNested()
    {
        Config nested = newConfig().set("k", newConfig());
        config.set("nested", nested.toString());
        assertConfigException(() -> config.deepCopy().getNested("nested"));
        assertConfigException(() -> config.deepCopy().getNestedOrGetEmpty("nested"));
        assertConfigException(() -> config.deepCopy().getNestedOrSetEmpty("nested"));
        assertConfigException(() -> config.deepCopy().getOptionalNested("nested"));
        assertThat(config.deepCopy().parseNested("nested"), is(nested));
        assertThat(config.deepCopy().parseNestedOrGetEmpty("nested"), is(nested));
    }

    @Test
    public void verifySetGetList()
    {
        List<String> nested = Arrays.asList("a", "b");
        config.set("nested", nested);
        assertThat(config.deepCopy().getList("nested", String.class), is(nested));
        assertThat(config.deepCopy().getList("nested", JsonNode.class),
                is(nested.stream().map(TextNode::valueOf).collect(Collectors.toList())));

        assertThat(config.deepCopy().getListOrEmpty("nested", String.class), is(nested));
        assertThat(config.deepCopy().getListOrEmpty("nested", JsonNode.class),
                is(nested.stream().map(TextNode::valueOf).collect(Collectors.toList())));

        assertThat(config.deepCopy().parseList("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseList("nested", JsonNode.class),
                is(nested.stream().map(TextNode::valueOf).collect(Collectors.toList())));

        assertThat(config.deepCopy().parseListOrGetEmpty("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseListOrGetEmpty("nested", JsonNode.class),
                is(nested.stream().map(TextNode::valueOf).collect(Collectors.toList())));
    }

    @Test
    public void verifySetGetListNotSet()
    {
        List<String> empty = Arrays.asList();
        assertConfigException(() -> config.deepCopy().getList("nested", String.class));
        assertConfigException(() -> config.deepCopy().getList("nested", JsonNode.class));
        assertThat(config.deepCopy().getListOrEmpty("nested", String.class), is(empty));

        assertConfigException(() -> config.deepCopy().parseList("nested", String.class));
        assertConfigException(() -> config.deepCopy().parseList("nested", JsonNode.class));
        assertThat(config.deepCopy().parseListOrGetEmpty("nested", String.class), is(empty));
    }

    @Test
    public void verifyParseList()
    {
        List<String> nested = Arrays.asList("a", "b");
        config.set("nested", "[\"a\", \"b\"]");
        assertConfigException(() -> config.deepCopy().getList("nested", String.class));
        assertConfigException(() -> config.deepCopy().getList("nested", JsonNode.class));

        assertConfigException(() -> config.deepCopy().getListOrEmpty("nested", String.class));
        assertConfigException(() -> config.deepCopy().getListOrEmpty("nested", JsonNode.class));

        assertThat(config.deepCopy().parseList("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseList("nested", JsonNode.class),
                is(nested.stream().map(TextNode::valueOf).collect(Collectors.toList())));

        assertThat(config.deepCopy().parseListOrGetEmpty("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseListOrGetEmpty("nested", JsonNode.class),
                is(nested.stream().map(TextNode::valueOf).collect(Collectors.toList())));
    }

    @Test
    public void verifyNullValueHandling()
    {
        config.set("null_value", JsonNodeFactory.instance.nullNode());

        assertThat(config.getKeys(), is(Arrays.asList("null_value")));
        assertThat(config.has("null_value"), is(true));
        assertThat(config.isEmpty(), is(false));

        assertConfigException(() -> config.get("null_value", String.class), "Parameter 'null_value' is required but null");
        assertConfigException(() -> config.get("null_value", Integer.class), "Parameter 'null_value' is required but null");
        assertThat(config.get("null_value", String.class, "default"), is("default"));
        assertThat(config.get("null_value", Integer.class, 100), is(100));
        assertThat(config.getOptional("null_value", String.class), is(Optional.<String>absent()));
        assertThat(config.getOptional("null_value", Integer.class), is(Optional.<Integer>absent()));

        assertThat(config.getListOrEmpty("null_value", String.class), is(ImmutableList.of()));
        assertThat(config.parseListOrGetEmpty("null_value", String.class), is(ImmutableList.of()));
        assertConfigException(() -> config.getList("null_value", String.class), "Parameter 'null_value' is required but null");
        assertConfigException(() -> config.parseList("null_value", String.class), "Parameter 'null_value' is required but null");

        assertThat(config.getMapOrEmpty("null_value", String.class, String.class), is(ImmutableMap.of()));
        assertConfigException(() -> config.getMap("null_value", String.class, String.class), "Parameter 'null_value' is required but null");

        assertThat(config.getNestedOrGetEmpty("null_value"), is(newConfig()));
        assertThat(config.deepCopy().getNestedOrSetEmpty("null_value"), is(newConfig()));
        assertThat(config.getOptionalNested("null_value"), is(Optional.absent()));
        assertThat(config.parseNestedOrGetEmpty("null_value"), is(newConfig()));
        assertConfigException(() -> config.getNested("null_value"), "Parameter 'null_value' must be an object");
        assertConfigException(() -> config.parseNested("null_value"), "Parameter 'null_value' must be an object");
    }

    @Test
    public void testGetOptional()
    {
        config.set("str", "s");

        // For migration of jackson-databind from 2.6 -> 2.8
        assertThat(config.getOptional("str", JsonNode.class).transform(JsonNode::deepCopy),
                is(Optional.of(TextNode.valueOf("s"))));
    }

    private void assertConfigException(Runnable func)
    {
        try {
            func.run();
            fail();
        }
        catch (ConfigException ex) {
            assertTrue(true);
        }
    }

    private void assertConfigException(Runnable func, String message)
    {
        try {
            func.run();
            fail();
        }
        catch (ConfigException ex) {
            assertThat(ex.getMessage(), containsString(message));
        }
    }
}
