package io.digdag.client.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class ConfigPath
{
    // TODO sada: This class should be able to represent any possible keys
    //            such as config['']['k.e.y']['!?']. However, because there
    //            are no immediate demands for it, it's not implemented yet.
    //            ConfigPath should implement escaping as documented in
    //            RFC 6901 JSON Pointer, or a JSR 374 JSON-P 1.1 implementation
    //            should replace ConfigPath.

    // String representation of ConfigPath is also used on database
    // (task_state_details.reset_store_params column)

    private static Pattern VALID_NAME = Pattern.compile("[a-zA-Z0-9_]+");
    private static Pattern VALID_EXPRESSION = Pattern.compile("(/[a-zA-Z0-9_]+)+");

    @JsonCreator
    public static ConfigPath parse(String expr)
    {
        if (!VALID_EXPRESSION.matcher(expr).matches()) {
            throw new IllegalArgumentException("Config key expression is invalid. Currently, only [a-zA-Z0-9_] characters are supported: " + expr);
        }

        String[] names = expr.substring(1).split("/", -1);

        return new ConfigPath(ImmutableList.copyOf(names));
    }

    private static ConfigPath get(String first, String... more)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add(first);
        builder.add(more);
        return new ConfigPath(builder.build());
    }

    private List<String> names;

    private ConfigPath(List<String> names)
    {
        Preconditions.checkArgument(names.size() >= 1, "Number of names must be larger than 1");
        for (String key : names) {
            Preconditions.checkArgument(VALID_NAME.matcher(key).matches(), "Invalid name in keys");
        }
        this.names = ImmutableList.copyOf(names);
    }

    public List<String> getNestNames()
    {
        return names.subList(0, names.size() - 1);
    }

    public String getLastName()
    {
        return names.get(names.size() - 1);
    }

    public List<String> getNames()
    {
        return names;
    }

    @Override
    @JsonValue
    public String toString()
    {
        return "/" + String.join("/", names);
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ConfigPath)) {
            return false;
        }
        ConfigPath o = (ConfigPath) other;
        return Objects.equals(names, o.names);
    }

    @Override
    public int hashCode()
    {
        return names.hashCode();
    }
}
