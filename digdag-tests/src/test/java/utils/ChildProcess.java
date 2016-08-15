package utils;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public class ChildProcess
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ChildProcess.class);

    private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactoryBuilder().setDaemon(true).build();

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private final ExecutorService executor = Executors.newCachedThreadPool(DAEMON_THREAD_FACTORY);

    private final Process process;

    private volatile boolean closed;

    ChildProcess(ChildProcessConfig config, Process process)
    {
        this.process = process;

        List<OutputStream> std = new ArrayList<>(ImmutableList.of(System.out));
        if (config.captureStdOut()) {
            std.add(out);
        }
        executor.execute(() -> copy(process.getInputStream(), std));

        List<OutputStream> err = new ArrayList<>(ImmutableList.of(System.err));
        if (config.captureStdErr()) {
            err.add(out);
        }
        executor.execute(() -> copy(process.getErrorStream(), err));
    }

    private static void copy(InputStream in, List<OutputStream> outs)
    {
        byte[] buffer = new byte[16 * 1024];
        try {
            while (true) {
                int r = in.read(buffer);
                if (r < 0) {
                    break;
                }
                for (OutputStream out : outs) {
                    out.write(buffer, 0, r);
                    out.flush();
                }
            }
        }
        catch (IOException e) {
            logger.error("Caught exception during byte stream copy", e);
        }
    }

    public int waitFor()
            throws InterruptedException
    {
        return process().waitFor();
    }

    public Process process()
    {
        return process;
    }

    @Override
    public void close()
            throws Exception
    {
        if (closed) {
            return;
        }
        closed = true;
        kill(process);
    }

    @Override
    protected void finalize()
            throws Throwable
    {
        super.finalize();
        close();
    }

    public String out(Charset charset)
    {
        return charset.decode(ByteBuffer.wrap(out.toByteArray())).toString();
    }

    public String err(Charset charset)
    {
        return charset.decode(ByteBuffer.wrap(err.toByteArray())).toString();
    }

    public String outUtf8()
    {
        return out(UTF_8);
    }

    public String errUtf8()
    {
        return err(UTF_8);
    }

    public int pid()
    {
        return pid(process);
    }

    private static void kill(Process p)
    {
        if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
            int pid = pid(p);
            if (pid != -1) {
                String[] cmd = {"kill", "-9", Integer.toString(pid)};
                try {
                    Runtime.getRuntime().exec(cmd);
                }
                catch (IOException e) {
                    logger.warn("command failed: {}", asList(cmd), e);
                }
            }
            p.destroyForcibly();
        }
    }

    private static int pid(Process p)
    {
        if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
            try {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                return f.getInt(p);
            }
            catch (Exception ignore) {
            }
        }
        return -1;
    }

    static class Trampoline
    {
        private static final String NAME = ManagementFactory.getRuntimeMXBean().getName();

        private static class Watchdog
                extends Thread
        {

            Watchdog()
            {
                setDaemon(false);
            }

            @Override
            public void run()
            {
                // Wait for parent to exit.
                try {
                    while (true) {
                        int c = System.in.read();
                        if (c == -1) {
                            break;
                        }
                    }
                }
                catch (IOException e) {
                    errPrefix();
                    e.printStackTrace(System.err);
                    System.err.flush();
                }
                System.err.println();
                err("parent exited: child process exiting");
                // Exit with non-zero status code to skip shutdown hooks
                System.exit(-1);
            }
        }

        public static void main(String... trampolineArgs)
        {
            Trampoline.Watchdog watchdog = new Trampoline.Watchdog();
            watchdog.setDaemon(true);
            watchdog.start();
            if (trampolineArgs.length < 1) {
                throw new AssertionError();
            }
            String mainClassName = trampolineArgs[0];
            Class<?> mainClass;
            try {
                mainClass = Class.forName(mainClassName);
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
                return;
            }
            Method main = null;
            for (Method method : mainClass.getDeclaredMethods()) {
                if (method.getName().equals("main")) {
                    if (method.getParameterCount() != 1) {
                        continue;
                    }
                    if (method.getParameterTypes()[0] != String[].class) {
                        continue;
                    }
                    int modifiers = method.getModifiers();
                    if (!Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers)) {
                        continue;
                    }
                    main = method;
                    break;
                }
            }
            if (main == null) {
                System.err.println("main method not found in class: " + mainClass);
                System.exit(2);
                return;
            }
            Object[] args = Arrays.copyOfRange(trampolineArgs, 1, trampolineArgs.length);
            try {
                main.invoke(null, (Object) args);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
                System.exit(3);
            }
        }

        private static void err(String message)
        {
            errPrefix();
            System.err.println(message);
            System.err.flush();
        }

        private static void errPrefix()
        {
            System.err.print(LocalTime.now() + " [" + NAME + "] " + ChildProcessFactory.class.getName() + ": ");
        }
    }
}
