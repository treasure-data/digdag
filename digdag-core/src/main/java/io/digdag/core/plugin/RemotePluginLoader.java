package io.digdag.core.plugin;

import java.util.List;
import java.util.stream.Collectors;
import java.util.ServiceConfigurationError;
import java.io.File;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.file.Path;
import com.google.common.collect.ImmutableList;

import io.digdag.commons.guava.ThrowablesUtil;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

import io.digdag.spi.Plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.core.plugin.LocalPluginLoader.lookupPlugins;

public class RemotePluginLoader
        implements PluginLoader
{
    private static final Logger logger = LoggerFactory.getLogger(RemotePluginLoader.class);

    private static final List<RemoteRepository> DEFAULT_REPOSITORIES = ImmutableList.copyOf(new RemoteRepository[] {
        new RemoteRepository.Builder("central", "default", "https://repo1.maven.org/maven2/").build(),
        new RemoteRepository.Builder("jcenter", "default", "https://jcenter.bintray.com/").build(),
    });

    private static final List<String> PARENT_FIRST_PACKAGES = ImmutableList.copyOf(new String[] {
            "io.digdag.spi",
            "io.digdag.client",
            "org.slf4j",
            "javax.inject",
            "com.google.common",
            "com.fasterxml.jackson.databind.annotation",
    });

    private static final List<String> PARENT_FIRST_RESOURCES = ImmutableList.copyOf(new String[] {
    });

    private static RepositorySystem newRepositorySystem()
    {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);

        //locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
        //    @Override
        //    public void serviceCreationFailed(Class<?> type, Class<?> impl, Throwable exception)
        //    {
        //        exception.printStackTrace();
        //    }
        //});

        return locator.getService(RepositorySystem.class);
    }

    private static RepositorySystemSession newRepositorySystemSession(RepositorySystem system, Path localRepositoryPath)
    {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

        LocalRepository localRepo = new LocalRepository(localRepositoryPath.toString());
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

        //session.setTransferListener(new ConsoleTransferListener());
        //session.setRepositoryListener(new ConsoleRepositoryListener());

        return session;
    }

    private final RepositorySystem system;
    private final RepositorySystemSession session;

    public RemotePluginLoader(Path localRepositoryPath)
    {
        this.system = newRepositorySystem();
        this.session = newRepositorySystemSession(system, localRepositoryPath);
    }

    @Override
    public PluginSet load(Spec spec)
    {
        if (spec.getDependencies().isEmpty()) {
            return PluginSet.empty();
        }

        ImmutableList.Builder<Plugin> builder = ImmutableList.builder();

        List<RemoteRepository> repositories = getRepositories(spec);

        for (String dep : spec.getDependencies()) {
            // download artifacts, and/or resolve local-repository references to them
            logger.debug("Loading plugin {}", dep);
            List<ArtifactResult> artifactResults = resolveArtifacts(repositories, dep);

            logger.debug("Classpath of plugin {}: {}", dep,
                    artifactResults.stream().map(a -> a.getArtifact().getFile().toString())
                    .collect(Collectors.joining(File.pathSeparator)));

            ClassLoader pluginClassLoader = buildPluginClassLoader(artifactResults);
            try {
                List<Plugin> plugins = lookupPlugins(pluginClassLoader);
                if (plugins.isEmpty()) {
                    logger.warn("No plugins found from a dependency '" + dep + "'");
                }
                else {
                    builder.addAll(plugins);
                }
            }
            catch (ServiceConfigurationError ex) {
                throw new RuntimeException("Failed to lookup io.digdag.spi.Plugin service from a dependency '" + dep + "'", ex);
            }
        }

        return new PluginSet(builder.build());
    }

    private ClassLoader buildPluginClassLoader(List<ArtifactResult> artifactResults)
    {
        ImmutableList.Builder<URL> urls = ImmutableList.builder();
        for (ArtifactResult artifactResult : artifactResults) {
            URL url;
            try {
                url = artifactResult.getArtifact().getFile().toPath().toUri().toURL();
            }
            catch (MalformedURLException ex) {
                throw ThrowablesUtil.propagate(ex);
            }
            urls.add(url);
        }
        return new PluginClassLoader(urls.build(), RemotePluginLoader.class.getClassLoader(),
                PARENT_FIRST_PACKAGES, PARENT_FIRST_RESOURCES);
    }

    private List<ArtifactResult> resolveArtifacts(List<RemoteRepository> repositories, String dep)
    {
        DependencyRequest depRequest = buildDependencyRequest(repositories, dep, JavaScopes.RUNTIME);
        try {
            return system.resolveDependencies(session, depRequest).getArtifactResults();
        }
        catch (DependencyResolutionException ex) {
            throw ThrowablesUtil.propagate(ex);
        }
    }

    private List<RemoteRepository> getRepositories(Spec spec)
    {
        ImmutableList.Builder<RemoteRepository> builder = ImmutableList.builder();

        builder.addAll(DEFAULT_REPOSITORIES);

        int i = 1;
        for (String repo : spec.getRepositories()) {
            builder.add(new RemoteRepository.Builder("repository-" + i, "default", repo).build());
            i++;
        }

        return builder.build();
    }

    private static DependencyRequest buildDependencyRequest(List<RemoteRepository> repositories, String identifier, String scope)
    {
        Artifact artifact = new DefaultArtifact(identifier);

        DependencyFilter classpathFlter = DependencyFilterUtils.classpathFilter(scope);

        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRoot(new Dependency(artifact, scope));
        collectRequest.setRepositories(repositories);

        return new DependencyRequest(collectRequest, classpathFlter);
    }
}
