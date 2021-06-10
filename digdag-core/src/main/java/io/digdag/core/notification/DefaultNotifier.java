package io.digdag.core.notification;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.digdag.client.config.Config;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.NotificationSender;
import io.digdag.spi.Notifier;
import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.util.RetryExecutor.retryExecutor;

public class DefaultNotifier
        implements Notifier
{
    private static final String NOTIFICATION_TYPE = "notification.type";

    private static final String NOTIFICATION_RETRIES = "notification.retries";
    private static final String NOTIFICATION_MIN_RETRY_WAIT = "notification.min_retry_wait";
    private static final String NOTIFICATION_MAX_RETRY_WAIT = "notification.max_retry_wait";
    private static final int NOTIFICATION_RETRIES_DEFAULT = 10;
    private static final int NOTIFICATION_MIN_RETRY_WAIT_DEFAULT = 1000;
    private static final int NOTIFICATION_MAX_RETRY_WAIT_DEFAULT = 30000;

    private static Logger logger = LoggerFactory.getLogger(DefaultNotifier.class);

    private Injector injector;
    private final NotificationSender sender;
    private final int retries;
    private final int minRetryWait;
    private final int maxRetryWait;

    @Inject
    public DefaultNotifier(Config systemConfig, Injector injector)
    {
        this.injector = injector;
        Optional<String> type = systemConfig.getOptional(NOTIFICATION_TYPE, String.class);
        this.sender = type.isPresent() ? sender(type.get()) : null;
        this.retries = systemConfig.get(NOTIFICATION_RETRIES, int.class, NOTIFICATION_RETRIES_DEFAULT);
        this.minRetryWait = systemConfig.get(NOTIFICATION_MIN_RETRY_WAIT, int.class, NOTIFICATION_MIN_RETRY_WAIT_DEFAULT);
        this.maxRetryWait = systemConfig.get(NOTIFICATION_MAX_RETRY_WAIT, int.class, NOTIFICATION_MAX_RETRY_WAIT_DEFAULT);
    }

    private NotificationSender sender(String type)
    {
        return injector.getInstance(Key.get(NotificationSender.class, Names.named(type)));
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
                .withInitialRetryWait(minRetryWait)
                .withMaxRetryWait(maxRetryWait)
                .onRetry((exception, retryCount, retryLimit, retryWait) -> logger.warn("Sending notification failed: retry {} of {}", retryCount, retryLimit, exception))
                .withRetryLimit(retries);

        try {
            retryExecutor.run(() -> {
                try {
                    sender.sendNotification(notification);
                }
                catch (NotificationException e) {
                    throw ThrowablesUtil.propagate(e);
                }
            });
        }
        catch (RetryExecutor.RetryGiveupException e) {
            throw new NotificationException("Sending notification failed", e);
        }
    }
}
