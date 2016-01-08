package io.digdag.guice.rs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import com.google.inject.Inject;

public class GuiceRsCommandLine
        extends ArrayList<String>
{
    @Inject
    public GuiceRsCommandLine(@ForCommandLine String[] args)
    {
        super(Arrays.asList(args));
    }
}
