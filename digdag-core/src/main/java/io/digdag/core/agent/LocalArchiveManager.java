package io.digdag.core.agent;

import java.io.File;
import com.google.inject.Inject;
import com.google.common.base.Optional;

public class LocalArchiveManager
{
    @Inject
    public LocalArchiveManager()
    {
        // TODO take REST API client
    }

    public interface Action<T>
    {
        public T run(Optional<File> file);
    }

    public <T> T newArchiveDirectory(Action<T> func)
    {
        // TODO download archive file from server, and cache it locally. if it exists, extract it to an empty directory. otherwise, do nothing and pass Optional.absent
        return func.run(Optional.absent());
    }
}
