package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.google.common.io.CharStreams;
import io.digdag.cli.ConfigUtil;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSecretList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.client.api.SecretValidation.isValidSecretKey;
import static io.digdag.client.api.SecretValidation.isValidSecretValue;
import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class Secrets
        extends ClientCommand
{
    @Parameter(names = {"--project"})
    String projectName = null;

    @Parameter(names = {"--set"}, variableArity = true)
    List<String> set = new ArrayList<>();

    @Parameter(names = {"--delete"}, variableArity = true)
    List<String> delete = new ArrayList<>();

    @Parameter(names = {"--local"})
    boolean local = false;

    @Override
    public void mainWithClientException()
            throws Exception
    {
        if (args.size() != 0) {
            throw usage(null);
        }

        if (!set.isEmpty() && !delete.isEmpty()) {
            throw usage("Please specify only one of --set or --delete");
        }

        if (local) {
            localMain();
        }
        else if (projectName != null) {
            clientMain();
        }
        else {
            throw usage("Please specify either --project or --local");
        }
    }

    private void localMain()
            throws Exception
    {
        Path secretsDir = ConfigUtil.digdagConfigHome(env).resolve("secrets");

        if (set.isEmpty() && delete.isEmpty()) {
            listLocalSecrets(secretsDir);
        }
        else if (!set.isEmpty()) {
            setLocalSecrets(secretsDir, set);
        }
        else if (!delete.isEmpty()) {
            deleteLocalSecrets(secretsDir, delete);
        }
        else {
            throw new AssertionError();
        }
    }

    private void deleteLocalSecrets(Path secretsDir, List<String> delete)
            throws SystemExitException, IOException
    {
        Map<String, Boolean> deleteSecrets = parseDelete(delete);

        for (String key : deleteSecrets.keySet()) {
            Path secretFilePath = secretsDir.resolve(key);
            Files.deleteIfExists(secretFilePath);
            err.println("Secret '" + key + "' deleted");
        }
    }

    private void setLocalSecrets(Path secretsDir, List<String> set)
            throws SystemExitException, IOException
    {
        Files.createDirectories(secretsDir);
        Map<String, String> setSecrets = parseSet(set);
        for (Map.Entry<String, String> entry : setSecrets.entrySet()) {
            Path secretFilePath = secretsDir.resolve(entry.getKey());
            Files.write(secretFilePath, entry.getValue().getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);
            err.println("Secret '" + entry.getKey() + "' set");
        }
    }

    private void listLocalSecrets(Path secretsDir)
            throws IOException
    {
        try {
            Files.list(secretsDir).forEach(
                    p -> out.println(p.getFileName().toString()));
        }
        catch (NoSuchFileException ignore) {
        }
    }

    private void clientMain()
            throws Exception
    {
        DigdagClient client = buildClient();
        RestProject project = client.getProject(projectName);

        if (set.isEmpty() && delete.isEmpty()) {
            listProjectSecrets(client, project.getId());
        }
        else if (!set.isEmpty()) {
            setProjectSecrets(client, project.getId(), set);
        }
        else if (!delete.isEmpty()) {
            deleteProjectSecrets(client, project.getId(), delete);
        }
        else {
            throw new AssertionError();
        }
    }

    private void deleteProjectSecrets(DigdagClient client, Id projectId, List<String> delete)
            throws SystemExitException
    {
        Map<String, Boolean> deleteSecrets = parseDelete(delete);

        // Delete secrets
        for (String key : deleteSecrets.keySet()) {
            client.deleteProjectSecret(projectId, key);
            err.println("Secret '" + key + "' deleted");
        }
    }

    private void setProjectSecrets(DigdagClient client, Id projectId, List<String> set)
            throws Exception
    {
        Map<String, String> setSecrets = parseSet(set);

        // Set secrets
        for (Map.Entry<String, String> entry : setSecrets.entrySet()) {
            client.setProjectSecret(projectId, entry.getKey(), entry.getValue());
            err.println("Secret '" + entry.getKey() + "' set");
        }
    }

    private void listProjectSecrets(DigdagClient client, Id projectId)
    {
        RestSecretList secretList = client.listProjectSecrets(projectId);
        secretList.secrets().forEach(s -> out.println(s.key()));
    }

    private Map<String, Boolean> parseDelete(List<String> delete)
            throws SystemExitException
    {
        Map<String, Boolean> deleteSecrets = new LinkedHashMap<>();

        for (String key : delete) {
            if (key.isEmpty()) {
                throw usage(null);
            }
            if (deleteSecrets.containsKey(key)) {
                throw usage("Duplicate --delete secret key: '" + key + "'");
            }
            deleteSecrets.put(key, TRUE);
        }

        for (String key : deleteSecrets.keySet()) {
            if (!isValidSecretKey(key)) {
                throw usage("Invalid secret key: " + key);
            }
        }
        return deleteSecrets;
    }

    private Map<String, String> parseSet(List<String> set)
            throws SystemExitException, IOException
    {
        Map<String, String> setSecrets = new LinkedHashMap<>();

        boolean inConsumed = false;

        for (String s : set) {
            if (s.isEmpty()) {
                throw usage(null);
            }

            // Read a list of secrets from stdin or a file?
            if (s.charAt(0) == '@') {
                String filename = s.substring(1);
                if (filename.isEmpty()) {
                    throw usage(null);
                }
                Map<String, String> secrets;
                String source;
                if (filename.charAt(0) == '-') {
                    // Read stdin as yaml (or json)
                    if (filename.length() != 1) {
                        throw usage(null);
                    }
                    if (inConsumed) {
                        throw usage("Can only read once from stdin");
                    }
                    inConsumed = true;
                    source = "stdin";

                    YAMLFactory yaml = new YAMLFactory();
                    YAMLParser parser = yaml.createParser(in);
                    secrets = DigdagClient.objectMapper().readValue(parser, new TypeReference<Map<String, String>>() {});
                }
                else {
                    // Read secrets from file
                    source = "file: " + filename;
                    Map<String, String> result;
                    YAMLFactory yaml = new YAMLFactory();
                    try (YAMLParser parser = yaml.createParser(Files.newInputStream(Paths.get(filename)))) {
                        result = objectMapper().readValue(parser, new TypeReference<Map<String, String>>() {});
                    }
                    secrets = result;
                }
                for (Map.Entry<String, String> entry : secrets.entrySet()) {
                    if (entry.getKey().isEmpty()) {
                        throw usage("Invalid key/value in " + source);
                    }
                    if (setSecrets.containsKey(entry.getKey())) {
                        throw usage("Duplicate secret key: '" + entry.getKey() + "' in " + source);
                    }
                    setSecrets.put(entry.getKey(), entry.getValue());
                }
                continue;
            }

            // key=value ?
            int equalsIndex = s.indexOf('=');
            if (equalsIndex != -1) {
                String key = s.substring(0, equalsIndex);
                int sLength = s.length();
                if(equalsIndex + 1 == sLength) {
                    throw usage("Empty value for --set secret key: '" + key + "'");
                }
                String value = s.substring(equalsIndex + 1, sLength);
                if (key.isEmpty()) {
                    throw usage(null);
                }
                if (setSecrets.containsKey(key)) {
                    throw usage("Duplicate --set secret key: '" + key + "'");
                }

                // Read value from file?
                if (value.charAt(0) == '@') {
                    String filename = value.substring(1);
                    if (filename.isEmpty()) {
                        throw usage(null);
                    }
                    Path path = Paths.get(filename);
                    byte[] rawValue = Files.readAllBytes(path);
                    value = new String(rawValue, UTF_8);
                    setSecrets.put(key, value);
                    continue;
                }

                // Read value from stdin?
                if (value.charAt(0) == '-') {
                    if (value.length() != 1) {
                        throw usage(null);
                    }
                    if (inConsumed) {
                        throw usage("Can only read once from stdin");
                    }
                    inConsumed = true;
                    String input = CharStreams.toString(new InputStreamReader(in));
                    setSecrets.put(key, input);
                    continue;
                }

                // Just a regular key=value
                setSecrets.put(key, value);
                continue;
            }

            // Only the key name was specified. Read secret from console with echo disabled
            if (inConsumed) {
                throw usage("Stdin already read");
            }
            String value = new String(System.console().readPassword(s + ":"));
            setSecrets.put(s, value);
        }

        // Validate keys and values
        for (Map.Entry<String, String> entry : setSecrets.entrySet()) {
            if (!isValidSecretKey(entry.getKey())) {
                throw usage("Invalid secret key: " + entry.getKey());
            }
            if (!isValidSecretValue(entry.getValue())) {
                throw usage("Invalid secret value for key: " + entry.getKey());
            }
        }
        return setSecrets;
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " secrets [--local] [--project <project>] [--set <key>=<value>] [--delete key]");
        showCommonOptions();
        return systemExit(error);
    }
}
