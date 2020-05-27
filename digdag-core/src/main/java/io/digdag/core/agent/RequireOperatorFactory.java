package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.TaskCallbackApi.ProjectIdentifier;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

import static java.util.Locale.ENGLISH;

public class RequireOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(RequireOperatorFactory.class);

    private final TaskCallbackApi callback;

    @Inject
    public RequireOperatorFactory(TaskCallbackApi callback)
    {
        this.callback = callback;
    }

    public String getType()
    {
        return "require";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new RequireOperator(context, callback);
    }

    private static class RequireOperator
            extends BaseOperator
    {
        private final TaskCallbackApi callback;
        private ConfigFactory cf;

        private RequireOperator(OperatorContext context, TaskCallbackApi callback)
        {
            super(context);
            this.callback = callback;
            this.cf = request.getConfig().getFactory();
        }

        @Override
        public TaskResult runTask()
        {
            Config config = request.getConfig();
            Config localConfig = request.getLocalConfig();
            Config lastStateParams = request.getLastStateParams();
            logger.debug("YY lastStateParams: {}", lastStateParams);
            String workflowName = config.get("_command", String.class);
            Instant instant = config.get("session_time", Instant.class);
            boolean ignoreFailure = config.get("ignore_failure", boolean.class, false);
            boolean ignoreNoExistence = config.get("ignore_failure", boolean.class, false);
            OptionRerunOn rerunOn = OptionRerunOn.of(config.get("rerun_on", String.class, "none"));

            Optional<String> retryAttemptName = localConfig.getOptional("retry_attempt_name", String.class);
            if (lastStateParams.has("rerun_on_retry_attempt_name")) { // This parameter is set because of rerun_on parameter.
                retryAttemptName = lastStateParams.getOptional("rerun_on_retry_attempt_name", String.class);
            }
            logger.debug("retryAttemptName: {}", retryAttemptName.or("(none)"));
            Config overrideParams = config.getNestedOrGetEmpty("params");

            Optional<StoredSessionAttempt> attempt = Optional.absent();
            Optional<ResourceNotFoundException> resourceNotFoundException = Optional.absent();
            Optional<SessionAttemptConflictException> sessionAttemptConflictException = Optional.absent();
            Optional<ProjectIdentifier> projectIdentifier = Optional.absent();
            /**
             *  First of all, try to start attempt by startSession()
             *  If no attempt exists (no conflict), it return new StoredSessionAttempt.
             *    - set state param "require_kicked" to true.
             *    - task is retried to wait for done by nextPolling.
             *  If something errors happen, it will throw following exceptions.
             *    - ResourceNotFoundException ... workflow ,project name(or id) are wrong. -> process as deterministic error
             *    - ResourceLimitExceededException ... this exception should be deterministic error
             *    - SessionAttemptConflictException ... this is not error.
             *      - If the conflict attempt is still running, wait for until done by nextPolling.
             *      - If done, check the attempt is kicked by this require> or not by state param "require_kicked".
             *        - If not kicked by this require>, check result and rerun_on option and determine rerun or not.
             *          - if need to rerun, generate unique retry_attempt_name and set to "rerun_on_retry_attempt_name"
             *          - throw nextPolling and in next task, "rerun_on_retry_attempt_name" is used as retry_attempt_name and must succeed to create new attempt because it is unique.
             *        - Both kicked or not kicked, check the result and ignore_failure option
             *          - If ignore_failure is true or attempt finished successfully, require> op finished successfully
             *          - else finished with exception.
             */
            try {
                projectIdentifier = Optional.of(makeProjectIdentifier());

                attempt = Optional.of(callback.startSession(
                        context,
                        request.getSiteId(),
                        projectIdentifier.get(),
                        workflowName,
                        instant,
                        retryAttemptName,
                        overrideParams));
            }
            catch (ResourceNotFoundException ex) {
                resourceNotFoundException = Optional.of(ex);
            }
            catch (SessionAttemptConflictException ex) {
                sessionAttemptConflictException = Optional.of(ex);
            }
            catch (ResourceLimitExceededException ex) {
                throw new TaskExecutionException(ex);
            }

            if (sessionAttemptConflictException.isPresent()) { // submit failed and get the conflicted attempt
                logger.debug("startSession conflicted");
                StoredSessionAttempt conflictedAttempt = sessionAttemptConflictException.get().getConflictedSession();
                if (conflictedAttempt.getStateFlags().isDone()) {
                    // A flag to distinguish the attempt is kicked by require> or previous attempt.
                    boolean requireKicked = lastStateParams.get("require_kicked", boolean.class, false);
                    if (!requireKicked) {
                        // Check rerun_on option
                        // If we need to run, set last_state_param gen_retry_attempt_name flag and nextPolling
                        if (rerunOn == OptionRerunOn.ALL ||
                                rerunOn == OptionRerunOn.FAILED && !conflictedAttempt.getStateFlags().isSuccess()) {
                            //To force run, set flag gen_retry_attempt_name and do polling
                            ConfigElement nextState = ConfigElement.copyOf(lastStateParams.deepCopy().set("rerun_on_retry_attempt_name", UUID.randomUUID().toString()));
                            logger.debug("YY nextState:{}", nextState);
                            // TODO use exponential-backoff to calculate retry interval
                            throw TaskExecutionException.ofNextPolling(1, nextState);
                        }
                    }
                    if (!ignoreFailure && !conflictedAttempt.getStateFlags().isSuccess()) {
                        // ignore_failure is false and the attempt is in error state. Make this operator failed.
                        throw new TaskExecutionException(String.format(ENGLISH,
                                "Dependent workflow failed. Session id: %d, attempt id: %d",
                                conflictedAttempt.getSessionId(), conflictedAttempt.getId()));
                    }
                    return TaskResult.empty(cf);
                }
                else {
                    // Wait for finish running attempt
                    // TODO use exponential-backoff to calculate retry interval
                    throw TaskExecutionException.ofNextPolling(1, ConfigElement.copyOf(request.getLastStateParams()));

                }
            }
            else if ( resourceNotFoundException.isPresent()) {
                if (ignoreNoExistence) {
                    return TaskResult.empty(cf);
                }
                else {
                    throw new TaskExecutionException(String.format(ENGLISH, "Dependent workflow does not exist. %s, workflowName:%s",
                            projectIdentifier.transform((p)->p.toString()).or(""), workflowName ));
                }
            }
            else if (attempt.isPresent()) { // startSession succeeded and created new attempt
                ConfigElement nextState = ConfigElement.copyOf(request.getLastStateParams().deepCopy().set("require_kicked", true));
                // TODO use exponential-backoff to calculate retry interval
                throw TaskExecutionException.ofNextPolling(1, nextState);
                // set lastStateParam kickedByRequire
                // unset gen_retry_attempt_name
                // nextPolling
            }
            else {
                throw new RuntimeException(String.format(ENGLISH,
                        "Unexpected condition happened in require> operator.  %s, workflowName:%s",
                        projectIdentifier.transform((p)->p.toString()).or(""), workflowName));
            }
        }


        /**
         * Make ProjectIdentifier from parameters.
         * @return
         */
        private ProjectIdentifier makeProjectIdentifier()
        {
            Config config = request.getConfig();
            Config localConfig = request.getLocalConfig();
            int projectId = config.get("project_id", int.class);
            Optional<Integer> projectIdParam = localConfig.getOptional("project_id", Integer.class);
            Optional<String> projectNameParam = localConfig.getOptional("project_name", String.class);

            if (projectIdParam.isPresent() && projectNameParam.isPresent()) {
                throw new ConfigException("Both project_id and project_name can't be set");
            }
            ProjectIdentifier projectIdentifier;
            if (projectNameParam.isPresent()) {
                projectIdentifier = ProjectIdentifier.ofName(projectNameParam.get());
            }
            else if (projectIdParam.isPresent()) {
                projectIdentifier = ProjectIdentifier.ofId(projectIdParam.get());
            }
            else {
                projectIdentifier = ProjectIdentifier.ofId(projectId);
            }
            return projectIdentifier;
        }
    }

    enum OptionRerunOn
    {
        NONE("none"),
        FAILED( "failed"),
        ALL ("all");

        private final String name;
        OptionRerunOn(String name) { this.name = name; }
        static OptionRerunOn of(String name) {
            switch (name) {
                case "none":
                    return NONE;
                case "failed":
                    return FAILED;
                case "all":
                    return ALL;
                default:
                    throw new ConfigException("invalid rerun_on option:" + name);
            }
        }
    }


}
