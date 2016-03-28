package io.digdag.core.config;

import org.yaml.snakeyaml.error.YAMLException;

class DuplicateKeyYAMLException extends YAMLException
{
    public DuplicateKeyYAMLException()
    {
        super("duplicate key");
    }
}
