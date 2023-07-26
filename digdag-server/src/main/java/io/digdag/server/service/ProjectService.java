package io.digdag.server.service;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.api.RestProject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.TempFileManager;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ArchiveType;
import io.digdag.core.repository.Project;
import io.digdag.core.repository.ProjectControl;
import io.digdag.core.repository.ProjectMetadataMap;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.storage.ArchiveManager;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.server.rs.DuplicateInputStream;
import io.digdag.server.rs.RestModels;
import io.digdag.server.rs.project.ProjectClearScheduleParam;
import io.digdag.server.rs.project.PutProjectsValidator;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.SecretControlStore;
import io.digdag.spi.SecretControlStoreManager;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.ProjectContentTarget;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.util.Md5CountInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import javax.ws.rs.InternalServerErrorException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;

public class ProjectService
{
    private static int MAX_ARCHIVE_TOTAL_SIZE_LIMIT;
    private static final int DEFAULT_ARCHIVE_TOTAL_SIZE_LIMIT = 2 * 1024 * 1024;
    // TODO: we may want to limit bytes of one file for `MAX_ARCHIVE_FILE_SIZE_LIMIT ` in the future instead of total size limit.
    // See also: https://github.com/treasure-data/digdag/pull/994#discussion_r258402647
    private static int MAX_ARCHIVE_FILE_SIZE_LIMIT;
    private final ConfigFactory cf;
    private final WorkflowCompiler compiler;
    private final ArchiveManager archiveManager;
    private final TransactionManager tm;
    private final ProjectStoreManager rm;
    private final AccessController ac;
    private final SchedulerManager srm;
    private final TempFileManager tempFiles;
    private final SecretControlStoreManager scsp;
    private final ProjectArchiveLoader projectArchiveLoader;
    private final PutProjectsValidator putProjectsValidator;

    @Inject
    public ProjectService(
            final ConfigFactory cf,
            final WorkflowCompiler compiler,
            final ArchiveManager archiveManager,
            final ProjectStoreManager rm,
            final AccessController ac,
            final TempFileManager tempFiles,
            final TransactionManager tm,
            final SchedulerManager srm,
            final SecretControlStoreManager scsp,
            final ProjectArchiveLoader projectArchiveLoader,
            final Config systemConfig)
    {
        this.cf = cf;
        this.compiler = compiler;
        this.archiveManager = archiveManager;
        this.rm = rm;
        this.ac = ac;
        this.tempFiles = tempFiles;
        this.tm = tm;
        this.srm = srm;
        this.scsp = scsp;
        this.projectArchiveLoader = projectArchiveLoader;
        this.putProjectsValidator = new PutProjectsValidator();
        MAX_ARCHIVE_TOTAL_SIZE_LIMIT = systemConfig.get("api.max_archive_total_size_limit", Integer.class, DEFAULT_ARCHIVE_TOTAL_SIZE_LIMIT);
        MAX_ARCHIVE_FILE_SIZE_LIMIT = MAX_ARCHIVE_TOTAL_SIZE_LIMIT;
    }

    public RestProject putProject(
            int siteId,
            Config userInfo,
            AuthenticatedUser authenticatedUser,
            Supplier<Map<String, String>> secrets,
            String name,
            String revision,
            InputStream body,
            long contentLength,
            String scheduleFromString,
            List<String> clearSchedules,
            boolean clearAllSchedules,
            Optional<List<String>> metadata)
            throws AccessControlException, ResourceConflictException, IOException, ResourceNotFoundException
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "project= is required");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(revision), "revision= is required");
        ProjectTarget projectTarget = ProjectTarget.of(siteId, name);
        ac.checkPutProject( // AccessControl
                projectTarget,
                authenticatedUser);

        return tm.<RestProject, IOException, ResourceConflictException, ResourceNotFoundException, AccessControlException>begin(() -> {
            Instant scheduleFrom = putProjectsValidator.validateAndGetScheduleFrom(scheduleFromString);
            int size = putProjectsValidator.validateAndGetContentLength(contentLength, MAX_ARCHIVE_TOTAL_SIZE_LIMIT);
            ProjectClearScheduleParam scheduleClearParam = new ProjectClearScheduleParam(clearSchedules, clearAllSchedules);

            try (TempFileManager.TempFile tempFile = tempFiles.createTempFile("upload-", ".tar.gz")) {
                // Read uploaded data to the temp file and following variables
                ArchiveMetadata meta;
                byte[] md5;
                try (OutputStream writeToTemp = Files.newOutputStream(tempFile.get())) {
                    Md5CountInputStream md5Count = new Md5CountInputStream(body);
                    meta = readArchiveMetadata(new DuplicateInputStream(md5Count, writeToTemp), name);
                    md5 = md5Count.getDigest();
                    if (md5Count.getCount() != contentLength) {
                        throw new IllegalArgumentException("Content-Length header doesn't match with uploaded data size");
                    }
                    validateWorkflowAndSchedule(userInfo, authenticatedUser, projectTarget, meta, scheduleClearParam);
                }

                ArchiveManager.Location location =
                        archiveManager.newArchiveLocation(siteId, name, revision, size);
                boolean storeInDb = location.getArchiveType().equals(ArchiveType.DB);

                if (!storeInDb) {
                    // upload to storage
                    try {
                        archiveManager
                                .getStorage(location.getArchiveType())
                                .put(location.getPath(), size, () -> Files.newInputStream(tempFile.get()));
                    }
                    catch (RuntimeException | IOException ex) {
                        throw new InternalServerErrorException("Failed to upload archive to a remote storage", ex);
                    }
                }

                // Getting secrets might fail. To avoid ending up with a project without secrets, get the secrets _before_ storing the project.
                // If getting the project secrets fails, the project will not be stored and the push can then be retried with the same revision.
                Map<String, String> secretsMap = secrets.get();

                RestProject restProject = rm.getProjectStore(siteId).putAndLockProject(
                        Project.of(name),
                        (store, storedProject) -> {
                            ProjectControl lockedProj = new ProjectControl(store, storedProject);
                            StoredRevision rev;
                            if (storeInDb) {
                                // store data in db
                                byte[] data = new byte[size];
                                try (InputStream in = Files.newInputStream(tempFile.get())) {
                                    ByteStreams.readFully(in, data);
                                }
                                catch (RuntimeException | IOException ex) {
                                    throw new InternalServerErrorException("Failed to load archive data in memory", ex);
                                }
                                rev = lockedProj.insertRevision(
                                        Revision.builderFromArchive(revision, meta, userInfo)
                                                .archiveType(ArchiveType.DB)
                                                .archivePath(Optional.absent())
                                                .archiveMd5(Optional.of(md5))
                                                .build()
                                );
                                lockedProj.insertRevisionArchiveData(rev.getId(), data);
                            }
                            else {
                                // store location of the uploaded file in db
                                rev = lockedProj.insertRevision(
                                        Revision.builderFromArchive(revision, meta, userInfo)
                                                .archiveType(location.getArchiveType())
                                                .archivePath(Optional.of(location.getPath()))
                                                .archiveMd5(Optional.of(md5))
                                                .build()
                                );
                            }

                            List<StoredWorkflowDefinition> defs =
                                    lockedProj.insertWorkflowDefinitions(rev,
                                            meta.getWorkflowList().get(),
                                            scheduleClearParam.checkClearList(meta.getWorkflowList().get().stream().map(w -> w.getName()).collect(Collectors.toList())),
                                            srm, scheduleFrom);

                            if (metadata.isPresent()) {
                                lockedProj.deleteProjectMetadata();
                                lockedProj.insertProjectMetadata(new ProjectMetadataMap(metadata.get()));
                            }

                            return RestModels.project(storedProject, rev);
                        });

                SecretControlStore secretControlStore = scsp.getSecretControlStore(siteId);
                secretsMap.forEach((k, v) -> secretControlStore.setProjectSecret(
                        RestModels.parseProjectId(restProject.getId()),
                        SecretScopes.PROJECT_DEFAULT,
                        k, v));
                return restProject;
            }
        }, IOException.class, ResourceConflictException.class, ResourceNotFoundException.class, AccessControlException.class);
    }

    private ArchiveMetadata readArchiveMetadata(InputStream in, String projectName)
            throws IOException
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(projectName), "projectName");
        try (TempFileManager.TempDir dir = tempFiles.createTempDir("push", projectName)) {
            long totalSize = 0;
            try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(in, 32*1024)))) {
                totalSize = extractConfigFiles(dir.get(), archive);
            }

            if (totalSize > MAX_ARCHIVE_TOTAL_SIZE_LIMIT) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                        "Total size of the archive exceeds limit (%d > %d bytes)",
                        totalSize, MAX_ARCHIVE_TOTAL_SIZE_LIMIT));
            }

            ProjectArchive archive = projectArchiveLoader.load(dir.get(), WorkflowResourceMatcher.defaultMatcher(), cf.create());

            return archive.getArchiveMetadata();
        }
    }


    // TODO: only write .dig files
    private long extractConfigFiles(java.nio.file.Path dir, TarArchiveInputStream archive)
            throws IOException
    {
        long totalSize = 0;
        TarArchiveEntry entry;
        while (true) {
            entry = archive.getNextTarEntry();
            if (entry == null) {
                break;
            }
            if (entry.isDirectory()) {
                // do nothing
            }
            else {
                putProjectsValidator.validateTarEntry(entry, MAX_ARCHIVE_FILE_SIZE_LIMIT);
                totalSize += entry.getSize();

                java.nio.file.Path file = dir.resolve(entry.getName());
                if (!file.normalize().startsWith(dir))
                    throw new IOException("Bad zip entry");
                Files.createDirectories(file.getParent());
                try (OutputStream out = Files.newOutputStream(file)) {
                    ByteStreams.copy(archive, out);
                }
            }
        }
        return totalSize;
    }

    private void validateWorkflowAndSchedule(
            Config userInfo,
            AuthenticatedUser authenticatedUser,
            ProjectTarget projectTarget,
            ArchiveMetadata meta,
            ProjectClearScheduleParam scheduleParam)
            throws AccessControlException
    {
        List<Config> taskConfigs = new ArrayList<>();
        WorkflowDefinitionList defs = meta.getWorkflowList();
        scheduleParam.validateWorkflowNames(defs.get().stream().map(wf -> wf.getName()).collect(Collectors.toList()));

        for (WorkflowDefinition def : defs.get()) {
            Workflow wf = compiler.compile(def.getName(), def.getConfig());

            // validate workflow and schedule
            for (WorkflowTask task : wf.getTasks()) {
                // raise an exception if task doesn't valid.
                Config taskConfig = task.getConfig();
                // collect task configs for later access control check
                taskConfigs.add(taskConfig);
            }
            Revision rev = Revision.builderFromArchive("check", meta, userInfo)
                    .archiveType(ArchiveType.NONE)
                    .build();
            // raise an exception if "schedule:" is invalid.
            srm.tryGetScheduler(rev, def);
        }

        ac.checkPutProjectContent(
                ProjectContentTarget.of(projectTarget, taskConfigs),
                authenticatedUser);
    }
}
