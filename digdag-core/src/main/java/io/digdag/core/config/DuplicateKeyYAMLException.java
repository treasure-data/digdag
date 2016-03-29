package io.digdag.core.config;

import java.util.Collection;
import org.yaml.snakeyaml.error.YAMLException;

class DuplicateKeyYAMLException extends YAMLException
{
    public DuplicateKeyYAMLException(Collection<String> keys)
    {
        super("Duplicated keys: " + keys);
    }
}
