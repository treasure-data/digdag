package io.digdag.standards.command.kubernetes;

import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodStatus;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

public class Pod
{
    public static Pod of(final io.fabric8.kubernetes.api.model.Pod pod)
    {
        return new Pod(pod);
    }

    private final io.fabric8.kubernetes.api.model.Pod pod;
    private final DateTimeFormatter formatter;

    private Pod(final io.fabric8.kubernetes.api.model.Pod pod)
    {
        this.pod = pod;
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH)
                .withZone(ZoneId.of("UTC"));
    }

    public String getName()
    {
        return getMetadata().getName();
    }

    public long getCreationTimestamp()
    {
        String creationTimestamp = getMetadata().getCreationTimestamp();
        Instant instant = Instant.from(formatter.parse(creationTimestamp));
        return instant.getEpochSecond();
    }

    public String getPhase()
    {
        return getStatus().getPhase();
    }

    public int getStatusCode()
    {
        // if the pod completed, we can use this method.
        final PodStatus podStatus = pod.getStatus();
        final List<ContainerStatus> containerStatusList = podStatus.getContainerStatuses();
        final ContainerStateTerminated terminated = containerStatusList.get(0).getState().getTerminated();
        return terminated.getExitCode();
    }

    @Override
    public String toString()
    {
        return pod.toString();
    }

    PodStatus getStatus()
    {
        return pod.getStatus();
    }

    ObjectMeta getMetadata()
    {
        return pod.getMetadata();
    }
}
