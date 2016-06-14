package io.digdag.spi;

public interface NotificationSender
{
    void sendNotification(Notification notification) throws NotificationException;
}
