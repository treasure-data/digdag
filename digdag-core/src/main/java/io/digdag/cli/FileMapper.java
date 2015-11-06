package io.digdag.cli;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FileMapper
{
    private final YAMLFactory yaml;
    private final ObjectMapper mapper;

    @Inject
    public FileMapper(ObjectMapper mapper)
    {
        this.yaml = new YAMLFactory();
        this.mapper = mapper;
    }

    public boolean checkExists(File file)
    {
        return file.exists() && file.length() > 0;
    }

    public <T> void writeFile(File file, T value)
    {
        try {
            // TODO use yaml if file path ends with yml, otherwise use json
            try (YAMLGenerator out = yaml.createGenerator(new FileOutputStream(file))) {
                // TODO write to a String first, then write to file. to not create partially-written broken file
                mapper.writeValue(out, value);
            }
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public <T> String toYaml(T value)
            throws IOException
    {
        StringWriter writer = new StringWriter();
        try (YAMLGenerator yamlOut = yaml.createGenerator(writer)) {
            mapper.writeValue(yamlOut, value);
        }
        return writer.toString();
    }

    public <T> T readFile(File file, Class<T> klass)
    {
        try {
            // TODO use yaml if file path ends with yml, otherwise use json
            try (YAMLParser in = yaml.createParser(new FileInputStream(file))) {
                return mapper.readValue(in, klass);
            }
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }
}
