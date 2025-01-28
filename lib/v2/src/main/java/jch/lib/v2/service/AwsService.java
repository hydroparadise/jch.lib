package jch.lib.v2.service;


import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;


import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public abstract class AwsService {
    static S3AsyncClient s3AsyncClient = null;


    public static S3AsyncClient getAsyncClient() {
        if (s3AsyncClient == null) {

            SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(50)                         // Adjust as needed.
                    .connectionTimeout(Duration.ofSeconds(60))  // Set the connection  timeout.
                    .readTimeout(Duration.ofSeconds(60)) // Set the read timeout.
                    .writeTimeout(Duration.ofSeconds(60)) // Set the write timeout.
                    .build();

            ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                    .apiCallTimeout(Duration.ofMinutes(2)) // Set the overall API call timeout.
                    .apiCallAttemptTimeout(Duration.ofSeconds(90)) // Set the individual call attempt timeout.
                    .retryStrategy(RetryMode.STANDARD)
                    .build();

            s3AsyncClient = S3AsyncClient.builder()
                    .region(Region.US_EAST_1)
                    .httpClient(httpClient)
                    .multipartEnabled(true)
                    .overrideConfiguration(overrideConfig)
                    .build();

        }
        return s3AsyncClient;
    }
}