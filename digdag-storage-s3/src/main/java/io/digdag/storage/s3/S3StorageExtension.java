package io.digdag.storage.s3;

import java.util.List;
import java.util.Arrays;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.StorageFactory;
import io.digdag.spi.Extension;

public class S3StorageExtension
        implements Extension
{
    @Override
    public List<Module> getModules()
    {
        return Arrays.asList(new S3StorageModule());
    }

    public static class S3StorageModule
        implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            Multibinder.newSetBinder(binder, StorageFactory.class)
                .addBinding().to(S3StorageFactory.class).in(Scopes.SINGLETON);
        }
    }
}
