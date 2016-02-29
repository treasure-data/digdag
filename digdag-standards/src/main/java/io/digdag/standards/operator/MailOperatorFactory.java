package io.digdag.standards.operator;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.IOException;
import java.nio.file.Path;
import javax.mail.Authenticator;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.Message.RecipientType;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeBodyPart;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MailOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(MailOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public MailOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "mail";
    }

    @Override
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new MailOperator(archivePath, request);
    }

    @Value.Immutable
    public interface AttachConfig
    {
        public String getPath();

        public String getContentType();

        public String getFileName();
    }

    private class MailOperator
            extends BaseOperator
    {
        public MailOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().setAllIfNotSet(
                    request.getConfig().getNestedOrGetEmpty("mail"));

            String body = templateEngine.templateCommand(archivePath, params, "body", UTF_8);
            String subject = params.get("subject", String.class);

            List<String> toList;
            try {
                toList = params.getList("to", String.class);
            }
            catch (ConfigException ex) {
                toList = ImmutableList.of(params.get("to", String.class));
            }

            boolean isHtml = params.get("html", boolean.class, false);

            List<AttachConfig> attachFiles = params.getListOrEmpty("attach_files", Config.class)
                .stream()
                .map((a) -> {
                    String path = a.get("path", String.class);
                    return ImmutableAttachConfig.builder()
                        .path(path)
                        .fileName(
                                a.getOptional("filename", String.class)
                                .or(path.substring(Math.max(path.lastIndexOf('/'), 0)))
                            )
                        .contentType(
                                a.getOptional("content_type", String.class)
                                .or("application/octet-stream")
                            )
                        .build();
                })
                .collect(Collectors.toList());

            Properties props = new Properties();

            props.setProperty("mail.smtp.host", params.get("host", String.class));
            props.setProperty("mail.smtp.port", params.get("port", String.class));
            props.put("mail.smtp.starttls.enable", Boolean.toString(params.get("tls", boolean.class, true)));
            if (params.get("ssl", boolean.class, false)) {
                props.put("mail.smtp.socketFactory.port", params.get("port", String.class));
                props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
                props.put("mail.smtp.socketFactory.fallback", "false");
            }

            props.setProperty("mail.debug", Boolean.toString(params.get("debug", boolean.class, false)));

            props.setProperty("mail.smtp.connectiontimeout", "10000");
            props.setProperty("mail.smtp.timeout", "60000");

            Session session;
            final String username = params.get("username", String.class, null);
            if (username != null) {
                props.setProperty("mail.smtp.auth", "true");
                final String password = params.get("password", String.class, "");
                session = Session.getInstance(props,
                        new Authenticator()
                        {
                            @Override
                            public PasswordAuthentication getPasswordAuthentication() {
                                return new PasswordAuthentication(username, password);
                            }
                        });
            }
            else {
                session = Session.getInstance(props);
            }

            MimeMessage msg = new MimeMessage(session);

            try {
                String from = getFlatOrNested("from");
                msg.setFrom(newAddress(from));
                msg.setSender(newAddress(from));

                msg.setRecipients(RecipientType.TO,
                        toList.stream()
                        .map(it -> newAddress(it))
                        .toArray(InternetAddress[]::new));

                msg.setSubject(subject);
                if (attachFiles.isEmpty()) {
                    msg.setText(body, "utf-8", isHtml ? "html" : "plain");
                }
                else {
                    MimeMultipart multipart = new MimeMultipart();

                    MimeBodyPart textPart = new MimeBodyPart();
                    textPart.setText(body, "utf-8", isHtml ? "html" : "plain");
                    multipart.addBodyPart(textPart);

                    for (AttachConfig attachFile : attachFiles) {
                        MimeBodyPart part = new MimeBodyPart();
                        part.attachFile(archive.getFile(attachFile.getPath()), attachFile.getContentType(), null);
                        part.setFileName(attachFile.getFileName());
                        multipart.addBodyPart(part);
                    }

                    msg.setContent(multipart);
                }
                Transport.send(msg);
            }
            catch (MessagingException | IOException ex) {
                throw new RuntimeException(ex);
            }

            return TaskResult.empty(request);
        }

        private String getFlatOrNested(String key)
        {
            Config config = request.getConfig();
            return config.getNestedOrGetEmpty("mail").getOptional(key, String.class)
                .or(() -> config.get(key, String.class));
        }

        private String getFlatOrNested(String key, String defaultValue)
        {
            Config config = request.getConfig();
            return config.getNestedOrGetEmpty("mail").getOptional(key, String.class)
                .or(() -> config.get(key, String.class, defaultValue));
        }

        private boolean getFlatOrNested(String key, boolean defaultValue)
        {
            Config config = request.getConfig();
            return config.getNestedOrGetEmpty("mail").getOptional(key, boolean.class)
                .or(() -> config.get(key, boolean.class, defaultValue));
        }

        private InternetAddress newAddress(String str)
        {
            try {
                return new InternetAddress(str);
            }
            catch (AddressException ex) {
                throw new ConfigException("Invalid address", ex);
            }
        }
    }
}
