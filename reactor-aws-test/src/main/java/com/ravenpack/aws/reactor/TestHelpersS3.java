package com.ravenpack.aws.reactor;

import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

@AllArgsConstructor
public class TestHelpersS3 {

    private final Localstack localstack;

    public S3AsyncClient getS3AsyncClient()
    {
        return S3AsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.S3))
                .credentialsProvider(localstack.getCredentials())
                .build();
    }

    public S3Client getS3Client()
    {
        return S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.S3))
                .credentialsProvider(localstack.getCredentials())
                .build();
    }

    public void s3Cleanup()
    {
        Flux.fromIterable(getS3Client().listBuckets().buckets())
                .flatMap(bucket -> emptyBucket(bucket.name())
                        .flatMap(it -> Mono.fromFuture(
                                getS3AsyncClient()
                                        .deleteBucket(DeleteBucketRequest.builder()
                                                .bucket(bucket.name())
                                                .build()))));
    }


    public String createBucket(String bucketName)
    {
        getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        return bucketName;
    }

    private Mono<Void> emptyBucket(String bucket)
    {
        return Mono.fromFuture(
                getS3AsyncClient().listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .build()))
                .map(ListObjectsV2Response::contents)
                .flatMapMany(Flux::fromIterable)
                .map(S3Object::key)
                .flatMap(key -> Mono.fromFuture(
                        getS3AsyncClient().deleteObject(DeleteObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build())))
                .flatMap(it -> Mono.fromFuture(
                        getS3AsyncClient().listObjectVersions(ListObjectVersionsRequest.builder()
                                .bucket(bucket).build()))
                        .map(ListObjectVersionsResponse::versions)
                        .flatMapMany(Flux::fromIterable)
                        .flatMap(objectVersion -> Mono.fromFuture(
                                getS3AsyncClient().deleteObject(DeleteObjectRequest.builder()
                                        .bucket(bucket)
                                        .key(objectVersion.key())
                                        .versionId(
                                                objectVersion.versionId())
                                        .build())
                        ))
                ).onErrorContinue((t, o) -> Mono.empty())
                .then();
    }


}
