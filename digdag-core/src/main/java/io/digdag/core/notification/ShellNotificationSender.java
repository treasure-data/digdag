package io.digdag.core.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.NotificationSender;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.ProcessBuilder.Redirect;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ShellNotificationSender
        implements NotificationSender
{
    private static final String NOTIFICATION_SHELL_COMMAND = "notification.shell.command";
    private static final String NOTIFICATION_SHELL_TIMEOUT = "notification.shell.timeout";
    private static final String NOTIFICATION_SHELL_LOGFILE = "notification.shell.logfile";
    private static final int NOTIFICATION_SHELL_TIMEOUT_DEFAULT = 30_000;

    private final String command;
    private final ObjectMapper mapper;
    private final int timeoutMs;
    private Optional<String> logFileName;

    @Inject
    public ShellNotificationSender(Config systemConfig, ObjectMapper mapper)
    {
        this.command = systemConfig.get(NOTIFICATION_SHELL_COMMAND, String.class);
        this.mapper = mapper;
        this.timeoutMs = systemConfig.get(NOTIFICATION_SHELL_TIMEOUT, int.class, NOTIFICATION_SHELL_TIMEOUT_DEFAULT);
        this.logFileName = Optional.fromNullable(systemConfig.get(NOTIFICATION_SHELL_LOGFILE, String.class, null));
    }

    @Override
    public void sendNotification(Notification notification)
            throws NotificationException
    {
        byte[] notificationJson;
        try {
            notificationJson = mapper.writeValueAsBytes(notification);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();

        Redirect redirectLog;
        if (logFileName.isPresent()) {
            redirectLog = Redirect.to(new File(logFileName.get()));
        } else {
            redirectLog = INHERIT;
        }

        ProcessBuilder processBuilder = new ProcessBuilder()
                .redirectOutput(redirectLog)
                .redirectError(redirectLog)
                .redirectInput(PIPE)
                .command("/bin/sh", "-c", command);

        try {
            Process process = processBuilder.start();

            executor.execute(() -> {
                try {
                    OutputStream stream = process.getOutputStream();
                    ByteStreams.copy(new ByteArrayInputStream(notificationJson), stream);
                    stream.flush();
                    stream.close();
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            });

            boolean exited = process.waitFor(timeoutMs, MILLISECONDS);
            if (!exited) {
                process.destroyForcibly();
                throw new NotificationException("Notification shell command timed out: " + command);
            }
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new NotificationException("Notification shell command failed: " + command + ", exit code = " + exitCode);
            }
        }
        catch (IOException e) {
            throw new NotificationException("Failed to execute notification shell command: " + command, e);
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}
