package io.digdag.cli;

import java.io.File;
import java.util.stream.Collectors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.util.List;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.*;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        Injector injector = Guice.createInjector(
                new ObjectMapperModule()
                    .registerModule(new GuavaModule())
                    .registerModule(new JodaModule()),
                new DatabaseModule()
                );

        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);

        final ConfigSource ast = loader.loadFile(new File("../demo.yml"));
        List<Workflow> workflows = ast.getKeys()
            .stream()
            .map(key -> compiler.compile(key, ast.getNested(key)))
            .collect(Collectors.toList());

        ObjectMapper om = injector.getInstance(ObjectMapper.class);
        String json = om.writeValueAsString(workflows);
        System.out.println(json);

        injector.getInstance(DatabaseMigrator.class).migrate();
        ConfigSourceFactory cf = injector.getInstance(ConfigSourceFactory.class);

        SessionStore sessionStore = injector.getInstance(SessionStoreManager.class).getSessionStore(0);
        StoredSession s = sessionStore.transaction(() ->
            sessionStore.putSession(
                    Session.sessionBuilder()
                    .uniqueName("ses1")
                    .sessionParams(cf.create())
                    .workflowId(0)
                    .build())
        );
    }
}
