package io.digdag.core.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.digdag.client.config.Config;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.NotificationSender;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ShellNotificationSender
        implements NotificationSender
{
    private static final Logger logger = LoggerFactory.getLogger(ShellNotificationSender.class);

    private static final String NOTIFICATION_SHELL_COMMAND = "notification.shell.command";
    private static final String NOTIFICATION_SHELL_TIMEOUT = "notification.shell.timeout";
    private static final int NOTIFICATION_SHELL_TIMEOUT_DEFAULT = 30_000;

    private final String command;
    private final ObjectMapper mapper;
    private final int timeoutMs;

    @Inject
    public ShellNotificationSender(Config systemConfig, ObjectMapper mapper)
    {
        this.command = systemConfig.get(NOTIFICATION_SHELL_COMMAND, String.class);
        this.mapper = mapper;
        this.timeoutMs = systemConfig.get(NOTIFICATION_SHELL_TIMEOUT, int.class, NOTIFICATION_SHELL_TIMEOUT_DEFAULT);
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

        ProcessBuilder processBuilder = new ProcessBuilder()
                .redirectErrorStream(true)
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

            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    logger.info("[ShellNotification](" + command + "): " + line);
                }
            }

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
