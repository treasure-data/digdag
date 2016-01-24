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
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.client.config.Config;
import io.digdag.core.LocalSite;

public class RevisionAutoReloader
{
    private static Logger logger = LoggerFactory.getLogger(RevisionAutoReloader.class);

    private final LocalSite localSite;
    private final ArgumentConfigLoader loader;
    private ScheduledExecutorService executor = null;
    private List<ReloadTarget> targets;

    @Inject
    public RevisionAutoReloader(LocalSite localSite, ArgumentConfigLoader loader)
    {
        this.localSite = localSite;
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
            ZoneId defaultTimeZone,
            Config overwriteParams)
        throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        ReloadTarget target = new ReloadTarget(file, defaultTimeZone, overwriteParams);
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
            executor.scheduleWithFixedDelay(() -> backgroundUpdateAll(), 30, 30, TimeUnit.SECONDS);
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
        private final File file;
        private final ZoneId defaultTimeZone;
        private final Config overwriteParams;
        private int lastRevId;
        private Dagfile lastDagfile;

        public ReloadTarget(File file, ZoneId defaultTimeZone, Config overwriteParams)
        {
            this.file = file;
            this.defaultTimeZone = defaultTimeZone;
            this.overwriteParams = overwriteParams;
            this.lastDagfile = null;
        }

        public void load()
            throws IOException, ResourceConflictException, ResourceNotFoundException
        {
            lastDagfile = readDagfile();
            localSite.storeWorkflows(
                    makeRevisionName(),
                    lastDagfile.getWorkflowList(),
                    Instant.now(),
                    lastDagfile.getDefaultParams()
                        .deepCopy()
                        .setAll(overwriteParams));
        }

        public void tryReload()
        {
            try {
                Dagfile dagfile = readDagfile();  // TODO optimize this code
                if (!dagfile.equals(lastDagfile)) {
                    logger.info("Reloading " + file);
                    localSite.storeWorkflows(
                            makeRevisionName(),
                            dagfile.getWorkflowList(),
                            Instant.now(),
                            dagfile.getDefaultParams()
                                .deepCopy()
                                .setAll(overwriteParams));
                    lastDagfile = dagfile;
                }
            }
            catch (RuntimeException | ResourceConflictException | ResourceNotFoundException | IOException ex) {
                logger.error("Failed to reload", ex);
            }
        }

        private Dagfile readDagfile()
            throws IOException
        {
            return loader.load(file, overwriteParams).convert(Dagfile.class);
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
