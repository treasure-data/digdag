package io.digdag.core.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Inject;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.NotificationSender;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.core.Headers;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;

public class HttpNotificationSender
        implements NotificationSender
{
    private static final String NOTIFICATION_HTTP_METHOD = "notification.http.method";
    private static final String NOTIFICATION_HTTP_METHOD_DEFAULT = "POST";
    private static final String NOTIFICATION_HTTP_URL = "notification.http.url";
    private static final String NOTIFICATION_HTTP_HEADERS_PREFIX = "notification.http.headers.";

    private final Client client;
    private final WebTarget target;
    private final Headers<Object> headers;
    private final String url;
    private final String method;

    @Inject
    public HttpNotificationSender(Config systemConfig)
    {
        this.client = new ResteasyClientBuilder()
                .register(new JacksonJsonProvider(objectMapper()))
                .connectionPoolSize(32)
                .build();
        this.url = systemConfig.get(NOTIFICATION_HTTP_URL, String.class);
        this.target = client.target(url);
        this.headers = headers(systemConfig);
        this.method = systemConfig.get(NOTIFICATION_HTTP_METHOD, String.class, NOTIFICATION_HTTP_METHOD_DEFAULT);
    }

    private Headers<Object> headers(Config systemConfig)
    {
        Headers<Object> headers = new Headers<>();
        systemConfig.getKeys().stream()
                .filter(k -> k.startsWith(NOTIFICATION_HTTP_HEADERS_PREFIX))
                .forEach(k -> {
                    String name = k.substring(
                            NOTIFICATION_HTTP_HEADERS_PREFIX.length());
                    String value = systemConfig.get(k, String.class);
                    headers.add(name, value);
                });
        return headers;
    }

    private static ObjectMapper objectMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new JacksonTimeModule());
        return mapper;
    }

    @Override
    public void sendNotification(Notification notification)
            throws NotificationException
    {
        Response response = target.request()
                .headers(headers)
                .method(method, Entity.entity(notification, "application/json"));

        response.close();

        if (response.getStatusInfo().getFamily() != SUCCESSFUL) {
            throw new NotificationException("Failed to send HTTP notification: status=" + response.getStatusInfo());
        }
    }
}
