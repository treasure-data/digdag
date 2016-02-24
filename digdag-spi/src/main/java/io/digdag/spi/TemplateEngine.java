package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.charset.Charset;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public interface TemplateEngine
{
    String template(Path basePath, String content, Config params)
        throws TemplateException;

    String templateFile(Path basePath, String fileName, Charset fileCharset, Config params)
        throws IOException, TemplateException;

    default String templateCommand(Path basePath, Config params, String aliasKey, Charset fileCharset)
    {
        if (params.has("_command")) {
            try {
                Config nested = params.getNested("_command");
                return nested.get("data", String.class);
            }
            catch (ConfigException notNested) {
                String command = params.get("_command", String.class);
                try {
                    return templateFile(basePath, command, fileCharset, params);
                }
                catch (IOException | TemplateException ex) {
                    throw new ConfigException("Failed to load a template file", ex);
                }
            }
        }
        else if (aliasKey != null) {
            return params.get(aliasKey, String.class);
        }
        else {
            return params.get("_command", String.class);  // this causes ConfigException with appropriate message
        }
    }
}
