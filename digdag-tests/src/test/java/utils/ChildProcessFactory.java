package utils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class ChildProcessFactory
{
    public ChildProcessFactory()
    {
    }

    public ChildProcess spawn(ChildProcessConfig config)
            throws IOException
    {
        String home = System.getProperty("java.home");
        String classPath = System.getProperty("java.class.path");
        Path java = Paths.get(home, "bin", "java").toAbsolutePath().normalize();

        List<String> processArgs = new ArrayList<>();
        processArgs.add(java.toString());
        processArgs.addAll(asList(
                "-cp", classPath,
                "-Xms128m", "-Xmx128m"));
        config.properties().forEach((key, value) -> processArgs.add("-D" + key + "=" + value));
        processArgs.add(ChildProcess.Trampoline.class.getName());
        processArgs.add(config.mainClass().getName());
        processArgs.addAll(config.args());

        ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

        if (config.workdir().isPresent()) {
            processBuilder.directory(config.workdir().get().toFile());
        }

        Process process = processBuilder.start();

        return new ChildProcess(config, process);
    }

    public ChildProcess spawn(Class<?> mainClass, String... args)
            throws IOException
    {
        return spawn(mainClass, asList(args));
    }

    private ChildProcess spawn(Class<?> mainClass, List<String> args)
            throws IOException
    {
        return spawn(ChildProcessConfig.builder()
                .mainClass(mainClass)
                .args(args)
                .build());
    }
}
