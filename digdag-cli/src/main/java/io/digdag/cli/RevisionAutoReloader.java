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
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.LocalSite;

class RevisionAutoReloader
{
    private static Logger logger = LoggerFactory.getLogger(RevisionAutoReloader.class);

    private final LocalSite localSite;
    private final ConfigFactory cf;
    private final ProjectArchiveLoader projectLoader;
    private ScheduledExecutorService executor = null;
    private List<ReloadTarget> targets;

    @Inject
    public RevisionAutoReloader(LocalSite localSite, ConfigFactory cf, ProjectArchiveLoader projectLoader)
    {
        this.localSite = localSite;
        this.cf = cf;
        this.projectLoader = projectLoader;
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

    void watch(ProjectArchive project)
        throws ResourceConflictException, ResourceNotFoundException
    {
        targets.add(new ReloadTarget(project));
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
                logger.error("Uncaught exception during reloading project at {}. Stopped monitoring this project.", target.getProjectPath(), ex);
                ite.remove();
            }
        }
    }

    private class ReloadTarget
    {
        private final Path projectPath;
        private final Config overrideParams;
        private ArchiveMetadata lastMetadata;
        private int lastRevId;

        private ReloadTarget(ProjectArchive project)
            throws ResourceConflictException, ResourceNotFoundException
        {
            this.projectPath = project.getProjectPath();
            this.overrideParams = project.getArchiveMetadata().getDefaultParams();
            storeProject(project, 1);
        }

        public Path getProjectPath()
        {
            return projectPath;
        }

        private void storeProject(ProjectArchive project, int revId)
            throws ResourceConflictException, ResourceNotFoundException
        {
            localSite.storeLocalWorkflows(
                    "default",
                    Integer.toString(revId),
                    project.getArchiveMetadata(),
                    Instant.now());
            this.lastMetadata = project.getArchiveMetadata();
            this.lastRevId = revId;
            logger.info("Added new revision {}", lastRevId);
        }

        private void tryReload()
        {
            try {
                ProjectArchive project = readProject();  // TODO optimize this code
                if (!project.getArchiveMetadata().equals(lastMetadata)) {
                    logger.info("Reloading {}", projectPath);
                    storeProject(project, lastRevId + 1);
                }
            }
            catch (RuntimeException | ResourceConflictException | ResourceNotFoundException | IOException ex) {
                logger.error("Failed to reload", ex);
            }
        }

        private ProjectArchive readProject()
            throws IOException
        {
            return projectLoader.load(projectPath, WorkflowResourceMatcher.defaultMatcher(), overrideParams);
        }
    }
}
