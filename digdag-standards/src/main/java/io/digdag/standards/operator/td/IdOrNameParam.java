package io.digdag.standards.operator.td;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Objects;
import java.util.regex.Pattern;
import static com.google.common.base.Preconditions.checkState;

public class IdOrNameParam
{
    private static final Pattern ID_PATTERN = Pattern.compile("0|[1-9][0-9]*");

    private final String name;
    private final long id;

    private IdOrNameParam(String name, long id)
    {
        this.name = name;
        this.id = id;
    }

    public boolean isName()
    {
        return name != null;
    }

    public boolean isId()
    {
        return name == null;
    }

    public String getName()
    {
        checkState(isName());
        return name;
    }

    public long getId()
    {
        checkState(isId());
        return id;
    }

    @JsonCreator
    public static IdOrNameParam of(JsonNode expr)
    {
        if (expr.isIntegralNumber()) {
            return new IdOrNameParam(null, expr.asLong());
        }
        else if (expr.isTextual()) {
            String text = expr.asText();
            if (ID_PATTERN.matcher(text).matches()) {
                long id;
                try {
                    id = Long.parseLong(text);
                }
                catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("Must be id or name: " + expr + " (integer out of range)");
                }
                return new IdOrNameParam(null, id);
            }
            else {
                return new IdOrNameParam(text, 0L);
            }
        }
        throw new IllegalArgumentException("Must be id or name: " + expr);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IdOrNameParam that = (IdOrNameParam) o;
        return Objects.equals(this.name, that.name) &&
            Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, id);
    }

    @Override
    public String toString()
    {
        if (isName()) {
            return name;
        }
        else {
            return Long.toString(id);
        }
    }

    @JsonValue
    public JsonNode toJsonNode()
    {
        if (isName()) {
            return JsonNodeFactory.instance.textNode(name);
        }
        else {
            return JsonNodeFactory.instance.numberNode(id);
        }
    }
}
