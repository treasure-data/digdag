package io.digdag.cli.client;

import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestSessionAttempt;
import static io.digdag.cli.Main.systemExit;

public class ShowAttempt
    extends ShowSession
{
    @Override
    protected boolean includeRetries()
    {
        return true;
    }
}
