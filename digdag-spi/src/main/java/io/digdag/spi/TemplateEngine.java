package io.digdag.spi;

import io.digdag.client.config.Config;

public interface TemplateEngine
{
    String template(String content, Config params)
        throws TemplateException;
}
