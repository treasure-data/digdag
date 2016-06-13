package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.NotificationSender;
import io.digdag.spi.Notifier;
import io.digdag.spi.TaskRequest;
import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static io.digdag.util.RetryExecutor.retryExecutor;

public class DefaultNotifier
        implements Notifier
{
    private static final String NOTIFICATION_TYPE = "notification.type";
    private static final String NOTIFICATION_TYPE_MAIL = "mail";
    private static final String NOTIFICATION_TYPE_HTTP = "http";
    private static final String NOTIFICATION_TYPE_SHELL = "shell";

    private static final String NOTIFICATION_RETRIES = "notification.retries";
    private static final String NOTIFICATION_MIN_RETRY_WAIT_MS = "notification.min_retry_wait_ms";
    private static final String NOTIFICATION_MAX_RETRY_WAIT_MS = "notification.max_retry_wait_ms";
    private static final int NOTIFICATION_RETRIES_DEFAULT = 10;
    private static final int NOTIFICATION_MIN_RETRY_WAIT_DEFAULT_MS = 1000;
    private static final int NOTIFICATION_MAX_RETRY_WAIT_DEFAULT_MS = 30000;

    private static Logger logger = LoggerFactory.getLogger(DefaultNotifier.class);

    private Injector injector;
    private final NotificationSender sender;
    private final int retries;
    private final int minRetryWaitMs;
    private final int maxRetryWaitMs;

    @Inject
    public DefaultNotifier(Config systemConfig, Injector injector)
    {
        this.injector = injector;
        Optional<String> type = systemConfig.getOptional(NOTIFICATION_TYPE, String.class);
        this.sender = type.isPresent() ? sender(type.get()) : null;
        this.retries = systemConfig.get(NOTIFICATION_RETRIES, int.class, NOTIFICATION_RETRIES_DEFAULT);
        this.minRetryWaitMs = systemConfig.get(NOTIFICATION_MIN_RETRY_WAIT_MS, int.class, NOTIFICATION_MIN_RETRY_WAIT_DEFAULT_MS);
        this.maxRetryWaitMs = systemConfig.get(NOTIFICATION_MAX_RETRY_WAIT_MS, int.class, NOTIFICATION_MAX_RETRY_WAIT_DEFAULT_MS);
    }

    private NotificationSender sender(String type)
    {
        // TODO: use key instead?
        switch (type) {
            case NOTIFICATION_TYPE_MAIL:
                return injector.getInstance(MailNotificationSender.class);
            case NOTIFICATION_TYPE_HTTP:
                return injector.getInstance(HttpNotificationSender.class);
            case NOTIFICATION_TYPE_SHELL:
                return injector.getInstance(ShellNotificationSender.class);
            default:
                throw new IllegalArgumentException("Unknown notification type: " + type);
        }
    }

    @Override
    public void sendNotification(Notification notification)
            throws NotificationException
    {
        logger.debug("Notification: {}", notification);

        if (sender == null) {
            return;
        }

        RetryExecutor retryExecutor = retryExecutor()
                .retryIf(exception -> true)
                .withInitialRetryWait(minRetryWaitMs)
                .withMaxRetryWait(maxRetryWaitMs)
                .onRetry((exception, retryCount, retryLimit, retryWait) -> logger.warn("Sending notification failed: retry {} of {}", retryCount, retryLimit, exception))
                .withRetryLimit(retries);

        try {
            retryExecutor.run(() -> {
                try {
                    sender.sendNotification(notification);
                }
                catch (NotificationException e) {
                    throw Throwables.propagate(e);
                }
            });
        }
        catch (RetryExecutor.RetryGiveupException e) {
            throw new NotificationException("Sending notification failed", e);
        }
    }
}
