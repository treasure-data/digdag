package io.digdag.core;

import java.util.List;
import com.google.inject.Module;

public interface Extension
{
    List<Module> getModules();
}
