package io.digdag.core;

import java.util.ServiceLoader;
import com.google.inject.Module;
import com.google.inject.Binder;
import io.digdag.spi.config.Config;

/**
 * ExtensionServiceLoaderModule loads Extensions using java.util.ServiceLoader
 * mechanism.
 * Jar packages providing an extension need to include
 * META-INF/services/io.digdag.core.Extension file. Contents of the file is
 * one-line text of the extension class name (e.g. com.example.MyPluginSourceExtension).
 */
public class ExtensionServiceLoaderModule
        implements Module
{
    private final ClassLoader classLoader;

    public ExtensionServiceLoaderModule()
    {
        this(ExtensionServiceLoaderModule.class.getClassLoader());
    }

    public ExtensionServiceLoaderModule(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public void configure(Binder binder)
    {
        ServiceLoader<Extension> serviceLoader = ServiceLoader.load(Extension.class, classLoader);
        for (Extension extension : serviceLoader) {
            for (Module module : extension.getModules()) {
                module.configure(binder);
            }
        }
    }
}
