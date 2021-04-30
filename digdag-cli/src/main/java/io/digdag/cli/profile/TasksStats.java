package io.digdag.cli.profile;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Optional;
import com.google.common.math.Stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@JsonSerialize(using = TasksStats.TasksStatsSerializer.class)
class TasksStats
{
    final Optional<Stats> stats;

    TasksStats(Optional<Stats> stats)
    {
        this.stats = stats;
    }

    static TasksStats of(Collection<Long> values)
    {
        if (values.isEmpty()) {
            return new TasksStats(Optional.absent());
        }
        else {
            return new TasksStats(Optional.of(Stats.of(values)));
        }
    }

    Long count()
    {
        return stats.transform(x -> Double.valueOf(x.count()).longValue()).orNull();
    }

    Long min()
    {
        return stats.transform(x -> Double.valueOf(x.min()).longValue()).orNull();
    }

    Long max()
    {
        return stats.transform(x -> Double.valueOf(x.max()).longValue()).orNull();
    }

    Long mean()
    {
        return stats.transform(x -> Double.valueOf(x.mean()).longValue()).orNull();
    }

    Long stdDev()
    {
        return stats.transform(x -> Double.valueOf(x.populationStandardDeviation()).longValue()).orNull();
    }

    @Override
    public String toString()
    {
        return "TasksStats{" +
                "stats=" + stats +
                '}';
    }

    static class Builder
    {
        private final List<Long> items = new ArrayList<>();

        void add(long item)
        {
            items.add(item);
        }

        TasksStats build()
        {
            return TasksStats.of(items);
        }
    }

    static class TasksStatsSerializer
            extends StdSerializer<TasksStats>
    {
        protected TasksStatsSerializer()
        {
            super(TasksStats.class);
        }

        @Override
        public void serialize(TasksStats tasksStats, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("count", tasksStats.count());
            jsonGenerator.writeObjectField("min", tasksStats.min());
            jsonGenerator.writeObjectField("max", tasksStats.max());
            jsonGenerator.writeObjectField("average", tasksStats.mean());
            jsonGenerator.writeObjectField("stddev", tasksStats.stdDev());
            jsonGenerator.writeEndObject();
        }
    }
}
