package org.seaborne.delta.server.s3;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.seaborne.delta.server.local.LocalServerConfig;
import uk.org.webcompere.systemstubs.rules.EnvironmentVariablesRule;

import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;

import static org.junit.Assert.*;

public class TestS3EnvironmentCredentials {

    @Rule
    public TestName testName = new TestName();

    @Rule
    public EnvironmentVariablesRule environmentVariablesRule = new EnvironmentVariablesRule(
            "AWS_ACCESS_KEY_ID", "key id",
            "AWS_SECRET_KEY", "key value"
    );

    @Test
    public void buildS3_environmentCredentials() {
        S3Config cfg = S3Config.create()
                .bucketName("test-bucket")
                .region("us-east-1")
                .endpoint("http://localhost:8080")
                .build();
        LocalServerConfig config = S3.configZkS3("", cfg);
        AmazonS3 aws = S3.buildS3(config);

        AWSCredentialsProvider provider = getCredentialsProvider(aws);
        AWSCredentials credentials = provider.getCredentials();

        assertTrue(credentials instanceof BasicAWSCredentials);
        assertEquals("key id", credentials.getAWSAccessKeyId());
        assertEquals("key value", credentials.getAWSSecretKey());
    }

    @Test
    public void buildS3_anonymousCredentials() {
        environmentVariablesRule.set("AWS_ACCESS_KEY_ID", "", "AWS_SECRET_KEY", "");

        S3Config cfg = S3Config.create()
                .bucketName("test-bucket")
                .region("us-east-1")
                .endpoint("http://localhost:8080")
                .build();
        LocalServerConfig config = S3.configZkS3("", cfg);
        AmazonS3 aws = S3.buildS3(config);

        AWSCredentialsProvider provider = getCredentialsProvider(aws);
        AWSCredentials credentials = provider.getCredentials();

        assertTrue(credentials instanceof AnonymousAWSCredentials);
        assertNull(credentials.getAWSAccessKeyId());
        assertNull(credentials.getAWSSecretKey());
    }

    @Test
    public void buildS3_defaultCredentials() {
        S3Config cfg = S3Config.create()
                .bucketName("test-bucket")
                .region("us-east-1")
                .build();
        LocalServerConfig config = S3.configZkS3("", cfg);
        AmazonS3 aws = S3.buildS3(config);

        AWSCredentialsProvider provider = getCredentialsProvider(aws);
        assertTrue(provider instanceof DefaultAWSCredentialsProviderChain);
    }

    private static AWSCredentialsProvider getCredentialsProvider(AmazonS3 s3) {
        try {
            Field field = s3.getClass().getDeclaredField("awsCredentialsProvider");
            field.setAccessible(true);
            return (AWSCredentialsProvider) field.get(s3);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new UndeclaredThrowableException(e);
        }
    }

}
