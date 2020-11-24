package io.digdag.cli.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.TypeLiteral;
import io.digdag.cli.ProgramName;
import io.digdag.cli.StdErr;
import io.digdag.cli.StdIn;
import io.digdag.cli.StdOut;
import io.digdag.cli.TimeUtil;
import io.digdag.client.api.Id;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.RestTask;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Environment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.InputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ShowTaskTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JacksonTimeModule())
        .registerModule(new GuavaModule());

    private static final Config EMPTY_CONFIG = new ConfigFactory(OBJECT_MAPPER).create();
    private static final Instant TIME_STAMP_0 = Instant.now().minusMillis(2000);
    private static final Instant TIME_STAMP_1 = Instant.now().minusMillis(1000);
    private static final Instant TIME_STAMP_2 = Instant.now().minusMillis(0);

    ShowTask showTask;

    @Mock
    PrintStream stdout;

    private static final List<RestTask> TASKS =
        ImmutableList.of(
            RestTask.builder()
                .id(Id.of("42"))
                .fullName("+test")
                .state("success")
                .startedAt(Optional.absent())
                .updatedAt(TIME_STAMP_2)
                .config(EMPTY_CONFIG)
                .parentId(Optional.absent())
                .upstreams(ImmutableList.of())
                .exportParams(EMPTY_CONFIG)
                .storeParams(EMPTY_CONFIG)
                .stateParams(EMPTY_CONFIG)
                .isGroup(true)
                .error(EMPTY_CONFIG)
                .build(),
            RestTask.builder()
                .id(Id.of("43"))
                .fullName("+test+start")
                .state("success")
                .startedAt(TIME_STAMP_0)
                .updatedAt(TIME_STAMP_1)
                .config(EMPTY_CONFIG)
                .parentId(Id.of("42"))
                .upstreams(ImmutableList.of())
                .exportParams(EMPTY_CONFIG)
                .storeParams(EMPTY_CONFIG)
                .stateParams(EMPTY_CONFIG)
                .isGroup(true)
                .error(EMPTY_CONFIG)
                .build()
        );

    private static final ShowTask.TasksSummary TASKS_SUMMARY_0 =
        new ShowTask.TasksSummary(
            9876,
            9870,
            9866,
            new ShowTask.TasksSummary.NullableLong(Optional.of(1234L)),
            new ShowTask.TasksSummary.NullableLong(Optional.of(222L)),
            new ShowTask.TasksSummary.NullableLong(Optional.of(5678L)),
            new ShowTask.TasksSummary.NullableLong(Optional.of(666L))
        );

    private static final ShowTask.TasksSummary TASKS_SUMMARY_1 =
        new ShowTask.TasksSummary(
            1234,
            1135,
            1133,
            new ShowTask.TasksSummary.NullableLong(Optional.absent()),
            new ShowTask.TasksSummary.NullableLong(Optional.absent()),
            new ShowTask.TasksSummary.NullableLong(Optional.absent()),
            new ShowTask.TasksSummary.NullableLong(Optional.absent())
        );

    @Before
    public void setUp()
    {
        showTask = Guice.createInjector(new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Environment.class).toInstance(Collections.emptyMap());
                bind(io.digdag.client.Version.class).toInstance(io.digdag.client.Version.parse("0.0.0"));
                bind(String.class).annotatedWith(ProgramName.class).toInstance("test");
                bind(InputStream.class).annotatedWith(StdIn.class).toInstance(System.in);
                bind(PrintStream.class).annotatedWith(StdOut.class).toInstance(stdout);
                bind(PrintStream.class).annotatedWith(StdErr.class).toInstance(System.err);
            }
        }).getInstance(ShowTask.class);
    }

    @Test
    public void showTaskWithTextPrinter()
    {
        ShowTask.Printer printer = new ShowTask.TextPrinter();
        printer.showTasks(showTask, TASKS);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(stdout, times(12 * 2 + 1)).println(argumentCaptor.capture());
        List<String> values = argumentCaptor.getAllValues();
        int i = 0;
        assertEquals("   id: 42", values.get(i++));
        assertEquals("   name: +test", values.get(i++));
        assertEquals("   state: success", values.get(i++));
        assertEquals("   started: ", values.get(i++));
        assertEquals("   updated: " + TimeUtil.formatTime(TIME_STAMP_2), values.get(i++));
        assertEquals("   config: {}", values.get(i++));
        assertEquals("   parent: null", values.get(i++));
        assertEquals("   upstreams: []", values.get(i++));
        assertEquals("   export params: {}", values.get(i++));
        assertEquals("   store params: {}", values.get(i++));
        assertEquals("   state params: {}", values.get(i++));
        assertEquals("", values.get(i++));
        assertEquals("   id: 43", values.get(i++));
        assertEquals("   name: +test+start", values.get(i++));
        assertEquals("   state: success", values.get(i++));
        assertEquals("   started: " + TimeUtil.formatTime(TIME_STAMP_0), values.get(i++));
        assertEquals("   updated: " + TimeUtil.formatTime(TIME_STAMP_1), values.get(i++));
        assertEquals("   config: {}", values.get(i++));
        assertEquals("   parent: 42", values.get(i++));
        assertEquals("   upstreams: []", values.get(i++));
        assertEquals("   export params: {}", values.get(i++));
        assertEquals("   store params: {}", values.get(i++));
        assertEquals("   state params: {}", values.get(i++));
        assertEquals("", values.get(i++));
        assertEquals("2 entries.", values.get(i++));
    }

    @Test
    public void showSummaryWithTextPrinter()
    {
        ShowTask.Printer printer = new ShowTask.TextPrinter();
        printer.showSummary(showTask, TASKS_SUMMARY_0);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(stdout, times(7)).println(argumentCaptor.capture());
        List<String> values = argumentCaptor.getAllValues();
        assertEquals("   totalTasks: 9876", values.get(0));
        assertEquals("   totalInvokedTasks: 9870", values.get(1));
        assertEquals("   totalSuccessTasks: 9866", values.get(2));
        assertEquals("   averageStartDelayMillis: 1234", values.get(3));
        assertEquals("   stdDevStartDelayMillis: 222", values.get(4));
        assertEquals("   averageExecTimeMillis: 5678", values.get(5));
        assertEquals("   stdDevExecTimeMillis: 666", values.get(6));
    }

    @Test
    public void showSummaryThatHasSomeEmptyValuesWithTextPrinter()
    {
        ShowTask.Printer printer = new ShowTask.TextPrinter();
        printer.showSummary(showTask, TASKS_SUMMARY_1);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(stdout, times(7)).println(argumentCaptor.capture());
        List<String> values = argumentCaptor.getAllValues();
        assertEquals("   totalTasks: 1234", values.get(0));
        assertEquals("   totalInvokedTasks: 1135", values.get(1));
        assertEquals("   totalSuccessTasks: 1133", values.get(2));
        assertEquals("   averageStartDelayMillis: N/A", values.get(3));
        assertEquals("   stdDevStartDelayMillis: N/A", values.get(4));
        assertEquals("   averageExecTimeMillis: N/A", values.get(5));
        assertEquals("   stdDevExecTimeMillis: N/A", values.get(6));
    }

    @Test
    public void showTaskWithJsonPrinter()
        throws JsonProcessingException
    {
        ShowTask.Printer printer = new ShowTask.JsonPrinter();
        printer.showTasks(showTask, TASKS);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(stdout, times(1)).println(argumentCaptor.capture());
        String value = argumentCaptor.getValue();
        assertEquals(OBJECT_MAPPER.writeValueAsString(TASKS), value);
    }

    @Test
    public void showSummaryWithJsonPrinter()
            throws JsonProcessingException
    {
        ShowTask.Printer printer = new ShowTask.JsonPrinter();
        printer.showSummary(showTask, TASKS_SUMMARY_0);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(stdout, times(1)).println(argumentCaptor.capture());
        String value = argumentCaptor.getValue();
        assertEquals(OBJECT_MAPPER.writeValueAsString(TASKS_SUMMARY_0), value);
    }

    @Test
    public void showSummaryThatHasSomeEmptyValuesWithJsonPrinter()
            throws JsonProcessingException
    {
        ShowTask.Printer printer = new ShowTask.JsonPrinter();
        printer.showSummary(showTask, TASKS_SUMMARY_1);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(stdout, times(1)).println(argumentCaptor.capture());
        String value = argumentCaptor.getValue();
        assertEquals(OBJECT_MAPPER.writeValueAsString(TASKS_SUMMARY_1), value);
    }
}
