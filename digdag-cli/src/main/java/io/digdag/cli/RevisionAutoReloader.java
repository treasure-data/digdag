package io.digdag.cli;

import java.util.List;
import java.util.Locale;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.core.archive.Dagfile;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.LocalSite;

public class RevisionAutoReloader
{
    private static Logger logger = LoggerFactory.getLogger(RevisionAutoReloader.class);

    private final LocalSite localSite;
    private final ConfigFactory cf;
    private final ProjectArchiveLoader loader;
    private ScheduledExecutorService executor = null;
    private List<ReloadTarget> targets;

    @Inject
    public RevisionAutoReloader(LocalSite localSite, ConfigFactory cf, ProjectArchiveLoader loader)
    {
        this.localSite = localSite;
        this.cf = cf;
        this.loader = loader;
        this.targets = new CopyOnWriteArrayList<>();
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    public void loadProject(List<Path> files, ZoneId defaultTimeZone)
        throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        ReloadTarget target = new ReloadTarget(files, defaultTimeZone);
        target.load();
        targets.add(target);
        startAutoReload();
    }

    private synchronized void startAutoReload()
    {
        if (executor == null) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("revision-auto-reloader-%d")
                    .build()
                    );
            executor.scheduleWithFixedDelay(() -> backgroundUpdateAll(), 5, 5, TimeUnit.SECONDS);
            this.executor = executor;
        }
    }

    private void backgroundUpdateAll()
    {
        Iterator<ReloadTarget> ite = targets.iterator();
        while (ite.hasNext()) {
            ReloadTarget target = ite.next();
            try {
                target.tryReload();
            }
            catch (Exception ex) {
                logger.error("Uncaught exception", ex);
                ite.remove();
            }
        }
    }

    private class ReloadTarget
    {
        private final List<Path> dagfilePaths;
        private final ZoneId defaultTimeZone;
        private int lastRevId;
        private ArchiveMetadata lastMetadata;

        public ReloadTarget(List<Path> dagfilePaths, ZoneId defaultTimeZone)
        {
            this.dagfilePaths = dagfilePaths;
            this.defaultTimeZone = defaultTimeZone;
            this.lastMetadata = null;
        }

        public void load()
            throws IOException, ResourceConflictException, ResourceNotFoundException
        {
            ProjectArchive project = readProject();
            localSite.storeLocalWorkflows(
                    "default",
                    makeRevisionName(),
                    project.getMetadata(),
                    Instant.now());
        }

        public void tryReload()
        {
            try {
                ProjectArchive project = readProject();  // TODO optimize this code
                ArchiveMetadata metadata = project.getMetadata();
                if (!metadata.equals(lastMetadata)) {
                    logger.info("Reloading {}", dagfilePaths);
                    StoredRevision rev = localSite.storeLocalWorkflows(
                            "default",
                            makeRevisionName(),
                            metadata,
                            Instant.now())
                        .getRevision();
                    lastMetadata = metadata;
                    logger.info("Added new revision {}", rev.getName());
                }
            }
            catch (RuntimeException | ResourceConflictException | ResourceNotFoundException | IOException ex) {
                logger.error("Failed to reload", ex);
            }
        }

        private ProjectArchive readProject()
            throws IOException
        {
            return loader.load(dagfilePaths, cf.create(), defaultTimeZone);
        }

        private String makeRevisionName()
        {
            DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ENGLISH)
                .withZone(defaultTimeZone);
            return formatter.format(Instant.now());
        }
    }
}
