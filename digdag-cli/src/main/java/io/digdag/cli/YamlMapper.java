package io.digdag.cli;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import com.google.inject.Inject;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class YamlMapper
{
    private final YAMLFactory yaml;
    private final ObjectMapper mapper;

    @Inject
    public YamlMapper(ObjectMapper mapper)
    {
        this.yaml = new YAMLFactory()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false);
        this.mapper = mapper;
    }

    public <T> void writeFile(File file, T value)
        throws IOException
    {
        file.getParentFile().mkdirs();
        // TODO use yaml if file path ends with yml, otherwise use json?
        try (YAMLGenerator out = yaml.createGenerator(new FileOutputStream(file))) {
            // TODO write to a String first, then write to file. to not create partially-written broken file
            mapper.writeValue(out, value);
        }
    }

    public <T> String toYaml(T value)
    {
        try {
            StringWriter writer = new StringWriter();
            try (YAMLGenerator out = yaml.createGenerator(writer)) {
                mapper.writeValue(out, value);
            }
            return writer.toString();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T> T readFile(File file, Class<T> type)
        throws IOException
    {
        // TODO use yaml if file path ends with yml, otherwise use json?
        try (YAMLParser out = yaml.createParser(new FileInputStream(file))) {
            // TODO write to a String first, then write to file. to not create partially-written broken file
            return mapper.readValue(out, type);
        }
    }
}
