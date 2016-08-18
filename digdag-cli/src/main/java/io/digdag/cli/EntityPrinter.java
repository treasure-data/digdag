package io.digdag.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.api.JacksonTimeModule;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class EntityPrinter<T>
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new GuavaModule())
            .registerModule(new JacksonTimeModule());

    private static final ObjectWriter YAML_WRITER = new ObjectMapper(new YAMLFactory()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false))
            .registerModule(new GuavaModule())
            .registerModule(new JacksonTimeModule())
            .writer();

    private final static ObjectWriter JSON_WRITER = MAPPER
            .writerWithDefaultPrettyPrinter();

    private final List<Field<T>> fields = new ArrayList<>();

    public void field(String name, Function<T, String> accessor)
    {
        fields.add(new Field<>(name, accessor));
    }

    public void print(OutputFormat f, T entity, OutputStream out)
            throws IOException
    {
        PrintStream ps = new PrintStream(out);
        print(f, entity, ps);
        ps.flush();
    }

    private void print(OutputFormat f, T entity, PrintStream ps)
            throws IOException
    {
        switch (f) {
            case TABLE: {
                for (Field<T> field : fields) {
                    ps.printf("%s: %s%n", field.name, field.accessor.apply(entity));
                }
            }
            break;
            case JSON: {
                String s = JSON_WRITER.writeValueAsString(entity);
                ps.println(s);
            }
            break;
            case YAML: {
                String s = YAML_WRITER.writeValueAsString(entity);
                ps.print(s);
            }
            break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class Field<T>
    {
        private final String name;
        private final Function<T, String> accessor;

        private Field(String name, Function<T, String> accessor)
        {
            this.name = name;
            this.accessor = accessor;
        }
    }
}
