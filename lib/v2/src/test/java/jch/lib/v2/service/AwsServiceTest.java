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


import org.junit.jupiter.api.Test;
//import static org.junit.jupiter.api.Assertions.*;

public class AwsServiceTest {

    static S3AsyncClient s3AsyncClient = null;

    public static void main(String[] args) {
        System.out.println("Hello AWS Service Test");
    }

    @Test
    void S3AsyncClient() {
        S3AsyncClient s3AsyncClient =  AwsService.getAsyncClient();
        assert s3AsyncClient!= null;
    }


}

