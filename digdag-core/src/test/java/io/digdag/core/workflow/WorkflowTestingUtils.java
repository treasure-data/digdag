package io.digdag.core.workflow;

import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.SchedulerFactory;
import io.digdag.spi.OperatorFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.database.DatabaseConfig;
import io.digdag.standards.StandardsExtension;
import static io.digdag.core.database.DatabaseTestingUtils.cleanDatabase;
import static io.digdag.core.database.DatabaseTestingUtils.getEnvironmentDatabaseConfig;

public class WorkflowTestingUtils
{
    private WorkflowTestingUtils() { }

    public static DigdagEmbed setupEmbed()
    {
        DigdagEmbed embed = new DigdagEmbed.Bootstrap()
            .withExtensionLoader(false)
            .addModules(new StandardsExtension().getModules())
            .addModules((binder) -> {
                Multibinder<OperatorFactory> operatorFactoryBinder = Multibinder.newSetBinder(binder, OperatorFactory.class);
                operatorFactoryBinder.addBinding().to(NoopOperatorFactory.class).in(Scopes.SINGLETON);
            })
            .overrideModulesWith((binder) -> {
                binder.bind(DatabaseConfig.class).toInstance(getEnvironmentDatabaseConfig());
            })
            .initializeCloseable();
        cleanDatabase(embed);
        return embed;
    }
}
