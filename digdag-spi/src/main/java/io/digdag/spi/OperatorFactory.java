package io.digdag.spi;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

public interface OperatorFactory
{
    String getType();

    default SecretAccessList getSecretAccessList()
    {
        return new SecretAccessList()
        {
            @Override
            public Set<String> getSecretKeys()
            {
                return Collections.emptySet();
            }
        };
    }

    Operator newOperator(OperatorContext context);
}
