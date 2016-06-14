package io.digdag.spi;

public interface Notifier
{
    void sendNotification(Notification notification) throws NotificationException;
}
