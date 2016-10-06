package io.digdag.client.config;

import java.util.List;
import java.util.Arrays;
import com.google.common.base.Optional;
import org.junit.Test;
import org.junit.Before;
import org.hamcrest.BaseMatcher;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.is;
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
        assertThat(config.deepCopy().getListOrEmpty("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseList("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseListOrGetEmpty("nested", String.class), is(nested));
    }

    @Test
    public void verifySetGetListNotSet()
    {
        List<String> empty = Arrays.asList();
        assertConfigException(() -> config.deepCopy().getList("nested", String.class));
        assertThat(config.deepCopy().getListOrEmpty("nested", String.class), is(empty));
        assertConfigException(() -> config.deepCopy().parseList("nested", String.class));
        assertThat(config.deepCopy().parseListOrGetEmpty("nested", String.class), is(empty));
    }

    @Test
    public void verifyParseList()
    {
        List<String> nested = Arrays.asList("a", "b");
        config.set("nested", "[\"a\", \"b\"]");
        assertConfigException(() -> config.deepCopy().getList("nested", String.class));
        assertConfigException(() -> config.deepCopy().getListOrEmpty("nested", String.class));
        assertThat(config.deepCopy().parseList("nested", String.class), is(nested));
        assertThat(config.deepCopy().parseListOrGetEmpty("nested", String.class), is(nested));
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
}
