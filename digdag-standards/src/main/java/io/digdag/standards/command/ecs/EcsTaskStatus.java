package io.digdag.standards.command.ecs;

/**
 * this class represents task lifecycle status on AWS ECS.
 * https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-lifecycle.html
 */
public enum EcsTaskStatus
{
    PROVISIONING(0, "provisioning"),
    PENDING(1, "pending"),
    ACTIVATING(2, "activating"),
    RUNNING(3, "running"),
    DEACTIVATING(4, "deactivating"),
    STOPPING(5, "stopping"),
    DEPROVISIONING(6, "deprovisioning"),
    STOPPED(7, "stopped");

    private final int index;
    private final String name;

    EcsTaskStatus(final int index, final String name)
    {
        this.index = index;
        this.name = name;
    }

    public int getIndex()
    {
        return index;
    }

    public String getName()
    {
        return name;
    }

    public boolean isFinished()
    {
        return isSameOrAfter(EcsTaskStatus.STOPPED);
    }

    public boolean isSameOrAfter(EcsTaskStatus another)
    {
        return another.index <= index;
    }

    public static EcsTaskStatus of(String value) {
        for (final EcsTaskStatus s : EcsTaskStatus.values()) {
            if (s.name.equalsIgnoreCase(value)) {
                return s;
            }
        }
        throw new IllegalStateException("Unknown task status " + value);
    }
}
