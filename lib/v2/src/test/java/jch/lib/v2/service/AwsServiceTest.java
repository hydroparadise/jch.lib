package jch.lib.v2.service;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;






import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;


import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;


import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;


import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;





import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;



public class AwsServiceTest {



    public static void main(String[] args) {
        System.out.println("Hello AWS Service Test");
    }


    void s3AsyncClientDownload() {
        String accessKey = "";
        String secretKey = "";
        String bucket = "";
        String prefix = "";
        Region region = Region.US_EAST_1;
        String key = prefix + "filename.txt";



        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey,secretKey);
        StaticCredentialsProvider credsProvider = StaticCredentialsProvider.create(awsCreds);

        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(50)
                .connectionTimeout(Duration.ofSeconds(60))
                .readTimeout(Duration.ofSeconds(60))
                .writeTimeout(Duration.ofSeconds(60))
                .build();


        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMinutes(2))
                .apiCallAttemptTimeout(Duration.ofSeconds(90))
                .retryStrategy(RetryMode.STANDARD)
                .build();

        S3AsyncClient s3AsyncCli = S3AsyncClient.builder()
                .multipartEnabled(true)
                .region(region)
                .credentialsProvider(credsProvider)
                .build();


        ExecutorService executor = Executors.newFixedThreadPool(10);
        try (S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncCli)
                .executor(executor)
                .build()) {

            DownloadFileRequest dlReq = DownloadFileRequest.builder()
                    .getObjectRequest(b -> b.bucket(bucket).key(key))
                    .build();

            FileDownload downloadFile = transferManager.downloadFile(dlReq);

            CompletedFileDownload downloadResult = downloadFile.completionFuture().join();

            //logger.info("Content length [{}]", downloadResult.response().contentLength());
            assert downloadResult.response().contentLength() > 0;
        }
    }

    void s3MultipartUpload() {


    }

    @Test
    void s3AsyncClient() {
        S3AsyncClient s3AsyncClient =  AwsService.getAsyncClient();
        assert s3AsyncClient!= null;
    }


}

