package io.digdag.standards.operator.redshift;

import io.digdag.client.config.ConfigException;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class UnloadConfigTest
{
    @Test
    public void setupWithPrefixDir()
            throws Exception
    {
        {
            RedshiftConnection.UnloadConfig unloadConfig = new RedshiftConnection.UnloadConfig();
            unloadConfig.to = "s3://my-bucket/my-prefix";
            unloadConfig.setupWithPrefixDir("1111-aaaa-2222-bbbb");
            assertThat(unloadConfig.toWithPrefixDir, is("s3://my-bucket/my-prefix/1111-aaaa-2222-bbbb_"));
            assertThat(unloadConfig.s3Bucket, is("my-bucket"));
            assertThat(unloadConfig.s3Prefix, is("my-prefix/1111-aaaa-2222-bbbb_"));
        }

        {
            RedshiftConnection.UnloadConfig unloadConfig = new RedshiftConnection.UnloadConfig();
            unloadConfig.to = "s3://my-bucket/my-prefix/";
            unloadConfig.setupWithPrefixDir("1111-aaaa-2222-bbbb");
            assertThat(unloadConfig.toWithPrefixDir, is("s3://my-bucket/my-prefix/1111-aaaa-2222-bbbb_"));
            assertThat(unloadConfig.s3Bucket, is("my-bucket"));
            assertThat(unloadConfig.s3Prefix, is("my-prefix/1111-aaaa-2222-bbbb_"));
        }

        {
            RedshiftConnection.UnloadConfig unloadConfig = new RedshiftConnection.UnloadConfig();
            unloadConfig.to = "s3://my-bucket";
            unloadConfig.setupWithPrefixDir("1111-aaaa-2222-bbbb");
            assertThat(unloadConfig.toWithPrefixDir, is("s3://my-bucket/1111-aaaa-2222-bbbb_"));
            assertThat(unloadConfig.s3Bucket, is("my-bucket"));
            assertThat(unloadConfig.s3Prefix, is("1111-aaaa-2222-bbbb_"));
        }

        {
            RedshiftConnection.UnloadConfig unloadConfig = new RedshiftConnection.UnloadConfig();
            unloadConfig.to = "my-bucket/my-prefix";
            try {
                unloadConfig.setupWithPrefixDir("1111-aaaa-2222-bbbb");
                assertTrue(false);
            }
            catch (ConfigException e) {
                assertTrue(true);
            }
        }

        {
            RedshiftConnection.UnloadConfig unloadConfig = new RedshiftConnection.UnloadConfig();
            unloadConfig.to = "s3:/my-bucket/my-prefix";
            try {
                unloadConfig.setupWithPrefixDir("1111-aaaa-2222-bbbb");
                assertTrue(false);
            }
            catch (ConfigException e) {
                assertTrue(true);
            }
        }

        {
            RedshiftConnection.UnloadConfig unloadConfig = new RedshiftConnection.UnloadConfig();
            unloadConfig.to = "s3:///my-prefix";
            try {
                unloadConfig.setupWithPrefixDir("1111-aaaa-2222-bbbb");
                assertTrue(false);
            }
            catch (ConfigException e) {
                assertTrue(true);
            }
        }
    }
}