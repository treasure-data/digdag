package io.digdag.core.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.ImmutableNotification;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
import io.digdag.spi.NotificationSender;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;

import javax.mail.Authenticator;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class MailNotificationSender
        implements NotificationSender
{
    private static final String NOTIFICATION_MAIL_FROM = "notification.mail.from";
    private static final String NOTIFICATION_MAIL_TO = "notification.mail.to";
    private static final String NOTIFICATION_MAIL_CC = "notification.mail.cc";
    private static final String NOTIFICATION_MAIL_BCC = "notification.mail.bcc";
    private static final String NOTIFICATION_MAIL_SUBJECT = "notification.mail.subject";
    private static final String NOTIFICATION_MAIL_SUBJECT_DEFAULT = "Digdag Notification";
    private static final String NOTIFICATION_MAIL_BODY_TEMPLATE_FILE = "notification.mail.body_template_file";
    private static final String NOTIFICATION_MAIL_HTML = "notification.mail.html";
    private static final boolean NOTIFICATION_MAIL_HTML_DEFAULT = false;

    private static final String NOTIFICATION_MAIL_BODY_TEMPLATE_DEFAULT = Joiner.on('\n').join(
            "Digdag Notification",
            "",
            "Message: ${message}",
            "Date: ${timestamp}",
            "",
            "Site Id: ${site_id}",
            "Project Name: ${project_name}",
            "Project Id: ${project_id}",
            "Workflow Name: ${workflow_name}",
            "Revision: ${revision}",
            "Attempt Id: ${attempt_id}",
            "Session Id: ${session_id}",
            "Task Name: ${task_name}",
            "Time Zone: ${timezone}",
            "Session Uuid: ${session_uuid}",
            "Session Time: ${session_time}"
    );

    private final List<String> to;
    private final TemplateEngine templateEngine;
    private final ObjectMapper mapper;
    private final List<String> cc;
    private final List<String> bcc;
    private final String subject;
    private final String bodyTemplate;
    private final Boolean isHtml;
    private final String from;
    private final Config config;

    @Inject
    public MailNotificationSender(Config systemConfig, TemplateEngine templateEngine, ObjectMapper mapper)
    {
        this.config = systemConfig.deepCopy();
        this.from = systemConfig.get(NOTIFICATION_MAIL_FROM, String.class);
        this.to = addressList(config, NOTIFICATION_MAIL_TO);
        this.templateEngine = templateEngine;
        this.mapper = mapper;
        config.setIfNotSet(NOTIFICATION_MAIL_CC, "");
        this.cc = addressList(config, NOTIFICATION_MAIL_CC);
        config.setIfNotSet(NOTIFICATION_MAIL_BCC, "");
        this.bcc = addressList(config, NOTIFICATION_MAIL_BCC);
        this.subject = config.get(NOTIFICATION_MAIL_SUBJECT, String.class, NOTIFICATION_MAIL_SUBJECT_DEFAULT);
        this.isHtml = config.get(NOTIFICATION_MAIL_HTML, boolean.class, NOTIFICATION_MAIL_HTML_DEFAULT);
        Optional<String> bodyTemplateFile = config.getOptional(NOTIFICATION_MAIL_BODY_TEMPLATE_FILE, String.class);
        this.bodyTemplate = bodyTemplateFile.transform(this::readFile).or(NOTIFICATION_MAIL_BODY_TEMPLATE_DEFAULT);

        selfCheck();
    }

    private void selfCheck()
    {
        // Verify that we can create a session
        createSession();

        // Verify that we can create a mail body
        ImmutableNotification notification = Notification.builder(Instant.now(), "message")
                .siteId(1)
                .projectName("project")
                .projectId(2)
                .workflowName("workflow")
                .revision("revision")
                .attemptId(3)
                .sessionId(4)
                .taskName("task")
                .timeZone(ZoneOffset.UTC)
                .sessionUuid(UUID.randomUUID())
                .sessionTime(OffsetDateTime.now())
                .workflowDefinitionId(5L)
                .build();
        try {
            body(notification);
        }
        catch (Exception e) {
            throw ThrowablesUtil.propagate(e);
        }
    }

    private String readFile(String s)
    {
        try {
            return new String(Files.readAllBytes(Paths.get(s)), "UTF-8");
        }
        catch (IOException e) {
            throw ThrowablesUtil.propagate(e);
        }
    }

    private List<String> addressList(Config systemConfig, String key)
    {
        List<String> toList;
        try {
            toList = systemConfig.getList(key, String.class);
        }
        catch (ConfigException ex) {
            String address = systemConfig.get(key, String.class).trim();
            if (address.isEmpty()) {
                return ImmutableList.of();
            }
            toList = ImmutableList.of(address);
        }
        return toList;
    }

    @Override
    public void sendNotification(Notification notification)
            throws NotificationException
    {
        Session session = createSession();

        MimeMessage msg = new MimeMessage(session);

        try {
            msg.setFrom(newAddress(from));
            msg.setSender(newAddress(from));

            msg.setRecipients(MimeMessage.RecipientType.TO, addresses(this.to));
            msg.setRecipients(MimeMessage.RecipientType.CC, addresses(this.cc));
            msg.setRecipients(MimeMessage.RecipientType.BCC, addresses(this.bcc));

            msg.setSubject(subject);
            msg.setText(body(notification), "utf-8", isHtml ? "html" : "plain");
            Transport.send(msg);
        }
        catch (MessagingException | IOException | TemplateException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    private InternetAddress[] addresses(List<String> addresses)
    {
        return addresses.stream()
                .map(this::newAddress)
                .toArray(InternetAddress[]::new);
    }

    private String body(Notification notification)
            throws JsonProcessingException, TemplateException
    {
        final String paramsJson = mapper.writeValueAsString(notification);
        Config params = config.getFactory().fromJsonString(paramsJson);
        return templateEngine.template(bodyTemplate, params);
    }

    private Session createSession()
    {
        Session session;

        Properties props = new Properties();

        props.setProperty("mail.smtp.host", config.get("notification.mail.host", String.class));
        props.setProperty("mail.smtp.port", config.get("notification.mail.port", String.class));
        props.put("mail.smtp.starttls.enable", Boolean.toString(config.get("notification.mail.tls", boolean.class, true)));
        if (config.get("notification.mail.ssl", boolean.class, false)) {
            props.put("mail.smtp.socketFactory.port", config.get("notification.mail.port", String.class));
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.socketFactory.fallback", "false");
        }

        props.setProperty("mail.debug", Boolean.toString(config.get("notification.mail.debug", boolean.class, false)));

        props.setProperty("mail.smtp.connectiontimeout", "10000");
        props.setProperty("mail.smtp.timeout", "60000");

        final String username = config.get("notification.mail.username", String.class, null);

        if (username != null) {
            props.setProperty("mail.smtp.auth", "true");
            final String password = config.get("notification.mail.password", String.class, "");
            session = Session.getInstance(props,
                    new Authenticator()
                    {
                        @Override
                        public PasswordAuthentication getPasswordAuthentication()
                        {
                            return new PasswordAuthentication(username, password);
                        }
                    });
        }
        else {
            session = Session.getInstance(props);
        }

        return session;
    }

    private InternetAddress newAddress(String address)
    {
        try {
            return new InternetAddress(address);
        }
        catch (AddressException ex) {
            throw new ConfigException("Invalid address", ex);
        }
    }
}
