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

import static java.lang.Math.abs;
import static java.util.Locale.ENGLISH;

public class RequireOperatorFactory
        implements OperatorFactory
{
    private static final int MAX_TASK_RETRY_INTERVAL = 10;
    // ToDo configurable in server config to run test solidly
    private static final int DELAY_SECONDS_KICK_IN_RESOURCE_LIMIT = 60 * 10; // 10 min.

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

            String workflowName = config.get("_command", String.class);
            Instant instant = config.get("session_time", Instant.class);
            boolean ignoreFailure = config.get("ignore_failure", boolean.class, false);
            OptionRerunOn rerunOn = OptionRerunOn.of(config.get("rerun_on", String.class, "none"));

            //retry_attempt_name is set from local config only.
            Optional<String> retryAttemptName = localConfig.getOptional("retry_attempt_name", String.class);
            if (lastStateParams.has("rerun_on_retry_attempt_name")) { // set for rerun_on parameter.
                retryAttemptName = lastStateParams.getOptional("rerun_on_retry_attempt_name", String.class);
            }

            Config overrideParams = config.getNestedOrGetEmpty("params");

            Optional<ProjectIdentifier> projectIdentifier = Optional.absent();
            /**
             *  Check "task_finish_success" state param. If exists, post-process will be executed and task will finish.
             *  Try to start attempt by startSession()
             *  If no attempt exists (no conflict), it returns new StoredSessionAttempt.
             *    - set state param "require_kicked" to true.
             *    - task is retried to wait for done by nextPolling.
             *  If something errors happen, it will throw following exceptions.
             *    - ResourceNotFoundException ... workflow ,project name(or id) are wrong. -> processed as deterministic error
             *    - ResourceLimitExceededException ... this exception should be deterministic error
             *    - SessionAttemptConflictException ... this is not error.
             *      - If the conflict attempt is still running, wait for until done by nextPolling.
             *      - If done, check the state param "require_kicked" and whether the attempt is kicked by this require> or not.
             *        - If not kicked by this require>, check result and rerun_on option and determine rerun or not.
             *          - if need to rerun, generate unique retry_attempt_name and set to "rerun_on_retry_attempt_name"
             *          - throw nextPolling and in next call of runTask(), "rerun_on_retry_attempt_name" is used as retry_attempt_name and must succeed to create new attempt because it is unique.
             *        - For both kicked and not kicked, check the result and ignore_failure option
             *          - If ignore_failure is true or attempt finished successfully, require> op finished successfully
             *          - else finished with exception.
             */
            try {
                //  Check whether the task finished
                if (lastStateParams.has("task_finish_success")) {
                    if (lastStateParams.get("task_finish_success", Boolean.class)) {
                        return TaskResult.empty(cf);
                    }
                    else {
                        throw new TaskExecutionException(String.format(ENGLISH,
                                "Dependent workflow failed. Session id: %d, attempt id: %d",
                                lastStateParams.get("target_session_id", Long.class, 0L),
                                lastStateParams.get("target_attempt_id", Long.class, 0L)
                                ));
                    }
                }

                projectIdentifier = Optional.of(makeProjectIdentifier());

                StoredSessionAttempt kickedAttempt = callback.startSession(
                        context,
                        request.getSiteId(),
                        projectIdentifier.get(),
                        workflowName,
                        instant,
                        retryAttemptName,
                        overrideParams);
                throw nextPolling(request.getLastStateParams().deepCopy()
                        .set("require_kicked", true)
                        .set("target_session_id", kickedAttempt.getSessionId())
                        .set("target_attempt_id", kickedAttempt.getId()));
            }
            catch (SessionAttemptConflictException ex) {
                return processAttempt(ex.getConflictedSession(), lastStateParams, rerunOn, ignoreFailure);
            }
            catch (ResourceNotFoundException ex) {
                throw new TaskExecutionException(String.format(ENGLISH, "Dependent workflow does not exist. %s, workflowName:%s",
                        projectIdentifier.transform(ProjectIdentifier::toString).or(""), workflowName));
            }
            catch (ResourceLimitExceededException ex) {
                logger.warn("Number of attempts or tasks exceed limit. Retry {} seconds later", DELAY_SECONDS_KICK_IN_RESOURCE_LIMIT);
                throw TaskExecutionException.ofNextPolling(DELAY_SECONDS_KICK_IN_RESOURCE_LIMIT, ConfigElement.copyOf(lastStateParams));
            }
        }

        private TaskResult processAttempt(StoredSessionAttempt attempt, Config lastStateParams, OptionRerunOn rerunOn, boolean ignoreFailure)
        {
            Config nextSateParams = lastStateParams.deepCopy()
                    .set("target_session_id", attempt.getSessionId())
                    .set("target_attempt_id", attempt.getId());

            if (attempt.getStateFlags().isDone()) {
                // A flag to distinguish whether the attempt is kicked by require> or previous attempt.
                boolean requireKicked = lastStateParams.get("require_kicked", boolean.class, false);
                if (!requireKicked &&  (
                        rerunOn == OptionRerunOn.ALL ||
                                rerunOn == OptionRerunOn.FAILED && !attempt.getStateFlags().isSuccess())) {

                    //To force run, set flag rerun_on_retry_attempt_name and do polling
                    throw nextPolling(lastStateParams.deepCopy().set("rerun_on_retry_attempt_name", UUID.randomUUID().toString()));
                }
                if (!ignoreFailure && !attempt.getStateFlags().isSuccess()) {
                    // ignore_failure is false and the attempt is in error state. Make this operator fail.
                    // In next polling, "task_finish_success" is checked and task will finish with failure.
                    nextSateParams.set("task_finish_success", false);
                    throw nextPolling(nextSateParams, 1);
                }
                // In next polling, "task_finish_success" is checked and task will finish successfully
                nextSateParams.set("task_finish_success", true);
                throw nextPolling(nextSateParams, 1);
            }
            else {
                // Wait for finish running attempt
                throw nextPolling(nextSateParams);
            }
        }

        private TaskExecutionException nextPolling(Config stateParams)
        {
            int iteration = stateParams.get("retry", int.class, 0);
            int interval = (int) Math.min(1 * Math.pow(2, iteration), MAX_TASK_RETRY_INTERVAL);
            return nextPolling(stateParams, interval);
        }

        private TaskExecutionException nextPolling(Config stateParams, int interval)
        {
            int iteration = stateParams.get("retry", int.class, 0);
            stateParams.set("retry", iteration + 1);
            return TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(stateParams));
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
        NONE, FAILED, ALL;

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
