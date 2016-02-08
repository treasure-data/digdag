package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.charset.Charset;
import io.digdag.client.config.Config;

public interface TemplateEngine
{
    String template(Path basePath, String content, Config params)
        throws TemplateException;

    String templateFile(Path basePath, String fileName, Charset fileCharset, Config params)
        throws IOException, TemplateException;
}
