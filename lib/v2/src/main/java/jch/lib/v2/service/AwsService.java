package jch.lib.v2.service;


import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
//import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.nio.file.Paths;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public abstract class AwsService {
    static S3AsyncClient s3AsyncClient = null;

    static {
        System.out.println("here!");
    }

    public Long downloadFile(S3TransferManager transferManager, String bucketName,
                             String key, String downloadedFileWithPath) {
        DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
                .getObjectRequest(b -> b.bucket(bucketName).key(key))
                .destination(Paths.get(downloadedFileWithPath))
                .build();
        FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);
        CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
        //logger.info("Content length [{}]", downloadResult.response().contentLength());
        return downloadResult.response().contentLength();
    }

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