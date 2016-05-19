package io.digdag.standards.operator.td;

import com.treasuredata.client.model.TDJobSummary;

public class TDJobException
        extends Exception
{
    private final String jobId;
    private final TDJobSummary summary;

    public TDJobException(String message,
            String jobId, TDJobSummary summary)
    {
        super(message);
        this.jobId = jobId;
        this.summary = summary;
    }

    public String getJobId()
    {
        return jobId;
    }

    public TDJobSummary getSummary()
    {
        return summary;
    }
}
