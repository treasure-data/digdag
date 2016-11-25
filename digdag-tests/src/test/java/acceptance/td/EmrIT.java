package acceptance.td;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.digdag.client.DigdagClient;
import io.digdag.core.config.YamlConfigLoader;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.TemporaryDigdagServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static utils.TestUtils.addResource;
import static utils.TestUtils.addWorkflow;
import static utils.TestUtils.attemptSuccess;
import static utils.TestUtils.createProject;
import static utils.TestUtils.expect;
import static utils.TestUtils.pushAndStart;
import static utils.TestUtils.pushProject;

public class EmrIT
{
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static final String S3_TEMP_BUCKET = System.getenv().getOrDefault("EMR_IT_S3_TEMP_BUCKET", "");

    private static final String AWS_ACCESS_KEY_ID = System.getenv().getOrDefault("EMR_IT_AWS_ACCESS_KEY_ID", "");
    private static final String AWS_SECRET_ACCESS_KEY = System.getenv().getOrDefault("EMR_IT_AWS_SECRET_ACCESS_KEY", "");

    protected final List<String> clusterIds = new ArrayList<>();

    protected AmazonElasticMapReduceClient emr;
    protected AmazonS3 s3;

    protected TemporaryDigdagServer server;

    protected Path projectDir;
    protected String projectName;
    protected int projectId;

    protected Path outfile;

    protected DigdagClient digdagClient;

    @Before
    public void setUp()
            throws Exception
    {
        assumeThat(S3_TEMP_BUCKET, not(isEmptyOrNullString()));
        assumeThat(AWS_ACCESS_KEY_ID, not(isEmptyOrNullString()));
        assumeThat(AWS_SECRET_ACCESS_KEY, not(isEmptyOrNullString()));

        AWSCredentials credentials = new BasicAWSCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY);

        emr = new AmazonElasticMapReduceClient(credentials);
        s3 = new AmazonS3Client(credentials);

        server = TemporaryDigdagServer.builder()
                .withRandomSecretEncryptionKey()
                .build();
        server.start();

        projectDir = folder.getRoot().toPath();
        createProject(projectDir);
        projectName = projectDir.getFileName().toString();
        projectId = pushProject(server.endpoint(), projectDir, projectName);

        outfile = folder.newFolder().toPath().resolve("outfile");

        digdagClient = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();

        digdagClient.setProjectSecret(projectId, "aws.access-key-id", AWS_ACCESS_KEY_ID);
        digdagClient.setProjectSecret(projectId, "aws.secret-access-key", AWS_SECRET_ACCESS_KEY);

        addResource(projectDir, "acceptance/emr/bootstrap_foo.sh");
        addResource(projectDir, "acceptance/emr/bootstrap_hello.sh");
        addResource(projectDir, "acceptance/emr/WordCount.jar");
        addResource(projectDir, "acceptance/emr/libhello.jar");
        addResource(projectDir, "acceptance/emr/simple.jar");
        addResource(projectDir, "acceptance/emr/hello.py");
        addResource(projectDir, "acceptance/emr/hello.sh");
        addResource(projectDir, "acceptance/emr/query.sql");
        addResource(projectDir, "acceptance/emr/pi.scala");
        addResource(projectDir, "acceptance/emr/emr_configuration.json");
        addWorkflow(projectDir, "acceptance/emr/emr.dig");
    }

    @After
    public void tearDownDigdagServer()
            throws Exception
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @After
    public void tearDownEmrClusters()
            throws Exception
    {
        if (!clusterIds.isEmpty()) {
            emr.terminateJobFlows(new TerminateJobFlowsRequest().withJobFlowIds(clusterIds));
        }
    }

    public static class EmrWithExistingClusterTest
            extends EmrIT
    {
        @Test
        public void test()
                throws Exception
        {
            Application spark = new Application()
                    .withName("Spark");

            RunJobFlowRequest request = new RunJobFlowRequest()
                    .withName("Digdag Test")
                    .withReleaseLabel("emr-5.2.0")
                    .withApplications(spark)
                    .withJobFlowRole("EMR_EC2_DefaultRole")
                    .withServiceRole("EMR_DefaultRole")
                    .withVisibleToAllUsers(true)
                    .withInstances(new JobFlowInstancesConfig()
                            .withEc2KeyName("digdag-test")
                            .withInstanceCount(1)
                            .withKeepJobFlowAliveWhenNoSteps(true)
                            .withMasterInstanceType("m3.xlarge")
                            .withSlaveInstanceType("m3.xlarge"));

            RunJobFlowResult result = emr.runJobFlow(request);

            String clusterId = result.getJobFlowId();

            clusterIds.add(clusterId);

            long attemptId = pushAndStart(server.endpoint(), projectDir, "emr", ImmutableMap.of(
                    "test_cluster", clusterId,
                    "outfile", outfile.toString()));
            expect(Duration.ofMinutes(15), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));
        }

        @Test
        public void manualTest()
                throws Exception
        {
            String clusterId = System.getenv("EMR_TEST_CLUSTER_ID");
            assumeThat(clusterId, not(Matchers.isEmptyOrNullString()));

            long attemptId = pushAndStart(server.endpoint(), projectDir, "emr", ImmutableMap.of(
                    "test_cluster", clusterId,
                    "outfile", outfile.toString()));
            expect(Duration.ofMinutes(15), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));
        }
    }

    public static class EmrWithNewClusterTest
            extends EmrIT
    {
        @Test
        public void test()
                throws Exception
        {
            String cluster = new YamlConfigLoader().loadString(Resources.toString(Resources.getResource("acceptance/emr/cluster.yaml"), UTF_8)).toString();
            long attemptId = pushAndStart(server.endpoint(), projectDir, "emr", ImmutableMap.of(
                    "test_cluster", cluster,
                    "outfile", outfile.toString()));
            expect(Duration.ofMinutes(30), attemptSuccess(server.endpoint(), attemptId));
            assertThat(Files.exists(outfile), is(true));
        }
    }
}
