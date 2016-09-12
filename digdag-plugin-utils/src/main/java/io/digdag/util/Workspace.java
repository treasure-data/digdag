package io.digdag.util;

import com.google.common.io.CharStreams;

import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.nio.file.Files;
import java.nio.charset.Charset;

public class Workspace
    implements Closeable
{
    private final Path path;
    private final List<String> tempFiles = new ArrayList<>();

    public Workspace(Path path)
    {
        this.path = path;
    }

    public Path getPath()
    {
        return path;
    }

    public String createTempFile(String prefix, String suffix)
        throws IOException
    {
        // file will be deleted by WorkspaceManager
        Path file = Files.createTempFile(getTempDir(), prefix, suffix);
        String relative = path.relativize(file).toString();
        tempFiles.add(relative);
        return relative;
    }

    public InputStream newInputStream(String relative)
        throws IOException
    {
        return Files.newInputStream(getPath(relative));
    }

    public BufferedReader newBufferedReader(String relative, Charset cs)
        throws IOException
    {
        return Files.newBufferedReader(getPath(relative), cs);
    }

    public OutputStream newOutputStream(String relative)
        throws IOException
    {
        return Files.newOutputStream(getPath(relative));
    }

    public BufferedWriter newBufferedWriter(String relative, Charset cs)
        throws IOException
    {
        return Files.newBufferedWriter(getPath(relative), cs);
    }

    public Path getPath(String relative)
    {
        return path.resolve(relative);
    }

    public File getFile(String relative)
    {
        return getPath(relative).toFile();
    }

    private synchronized Path getTempDir()
        throws IOException
    {
        Path dir = path.resolve(".digdag/tmp");
        Files.createDirectories(dir);
        return dir;
    }

    public String templateFile(TemplateEngine templateEngine, String fileName, Charset fileCharset, Config params)
        throws IOException, TemplateException
    {
        Path basePath = path.toAbsolutePath().normalize();
        Path absPath = basePath.resolve(fileName).normalize();
        if (!absPath.toString().startsWith(basePath.toString())) {
            throw new IllegalArgumentException("file name must not include ..: " + fileName);
        }

        try (BufferedReader reader = Files.newBufferedReader(absPath, fileCharset)) {
            String content = CharStreams.toString(reader);
            return templateEngine.template(content, params);
        }
    }

    public String templateCommand(TemplateEngine templateEngine, Config params, String aliasKey, Charset fileCharset)
    {
        if (params.has("_command")) {
            Config nested;
            try {
                nested = params.getNested("_command");
            }
            catch (ConfigException notNested) {
                String command = params.get("_command", String.class);
                try {
                    return templateFile(templateEngine, command, fileCharset, params);
                }
                catch (ConfigException ex) {
                    throw ex;
                }
                catch (TemplateException ex) {
                    throw new ConfigException("" + ex.getMessage() + " in " + command, ex);
                }
                catch (FileNotFoundException | NoSuchFileException ex) {
                    throw new ConfigException("File not found: " + ex.getMessage(), ex);
                }
                catch (RuntimeException | IOException ex) {
                    throw new ConfigException("Failed to read a template file: " + command + ": " + ex.getClass(), ex);
                }
            }
            // ${...} in nested parameters are already evaluated. no needs to call template.
            return nested.get("data", String.class);
        }
        else if (aliasKey != null) {
            return params.get(aliasKey, String.class);
        }
        else {
            return params.get("_command", String.class);  // this causes ConfigException with appropriate message
        }
    }

    @Override
    public void close()
    {
        for (String relative : tempFiles) {
            try {
                Files.deleteIfExists(getPath(relative));
            }
            catch (IOException ex) {
                // TODO show warning log
            }
        }
    }
}
