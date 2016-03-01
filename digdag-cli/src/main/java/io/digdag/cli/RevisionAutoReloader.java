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
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.core.repository.Dagfile;
import io.digdag.core.repository.ArchiveMetadata;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.LocalSite;

public class RevisionAutoReloader
{
    private static Logger logger = LoggerFactory.getLogger(RevisionAutoReloader.class);

    private final LocalSite localSite;
    private final ConfigFactory cf;
    private final ConfigLoaderManager loader;
    private ScheduledExecutorService executor = null;
    private List<ReloadTarget> targets;

    @Inject
    public RevisionAutoReloader(LocalSite localSite, ConfigFactory cf, ConfigLoaderManager loader)
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

    public void loadFile(File file,
            ZoneId defaultTimeZone)
        throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        ReloadTarget target = new ReloadTarget(file, defaultTimeZone);
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
        private final File dagfilePath;
        private final ZoneId defaultTimeZone;
        private int lastRevId;
        private Dagfile lastDagfile;

        public ReloadTarget(File dagfilePath, ZoneId defaultTimeZone)
        {
            this.dagfilePath = dagfilePath;
            this.defaultTimeZone = defaultTimeZone;
            this.lastDagfile = null;
        }

        public void load()
            throws IOException, ResourceConflictException, ResourceNotFoundException
        {
            lastDagfile = readDagfile();
            localSite.storeLocalWorkflows(
                    makeRevisionName(),
                    ArchiveMetadata.of(
                        lastDagfile.getWorkflowList(),
                        lastDagfile.getDefaultParams(),
                        lastDagfile.getDefaultTimeZone().or(defaultTimeZone)),
                    Instant.now());
        }

        public void tryReload()
        {
            try {
                Dagfile dagfile = readDagfile();  // TODO optimize this code
                if (!dagfile.equals(lastDagfile)) {
                    logger.info("Reloading {}", dagfilePath);
                    StoredRevision rev = localSite.storeLocalWorkflows(
                            makeRevisionName(),
                            ArchiveMetadata.of(
                                dagfile.getWorkflowList(),
                                dagfile.getDefaultParams(),
                                lastDagfile.getDefaultTimeZone().or(defaultTimeZone)),
                            Instant.now());
                    lastDagfile = dagfile;
                    logger.info("Added new revision {}", rev.getName());
                }
            }
            catch (RuntimeException | ResourceConflictException | ResourceNotFoundException | IOException ex) {
                logger.error("Failed to reload", ex);
            }
        }

        private Dagfile readDagfile()
            throws IOException
        {
            return loader.loadParameterizedFile(dagfilePath, cf.create()).convert(Dagfile.class);
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
