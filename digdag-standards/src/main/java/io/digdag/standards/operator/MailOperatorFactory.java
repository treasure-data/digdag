package io.digdag.standards.operator;

import java.util.List;
import java.util.Properties;
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
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public class MailOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(MailOperatorFactory.class);

    private final CommandExecutor exec;

    @Inject
    public MailOperatorFactory(CommandExecutor exec)
    {
        this.exec = exec;
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
            Config config = request.getConfig();
            Config params =
                config.getNestedOrGetEmpty("mail").deepCopy()
                .setAll(config);

            String subject = config.getOptional("_command", String.class)
                .or(() -> config.get("subject", String.class));
            String body = config.get("body", String.class, "");

            List<String> toList;
            try {
                toList = params.getList("to", String.class);
            }
            catch (ConfigException ex) {
                toList = ImmutableList.of(params.get("to", String.class));
            }

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
                msg.setText(body);

                Transport.send(msg);
            }
            catch (MessagingException ex) {
                throw new RuntimeException(ex);
            }

            return TaskResult.empty(request.getConfig().getFactory());
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
