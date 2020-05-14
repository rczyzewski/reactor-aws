package com.ravenpack.aws.reactor.s3;

import com.ravenpack.aws.reactor.AwsTestLifecycle;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class RxS3ImplIT
{
    private static final String KEY = "objectkey";
    private static final String CONTENT_TYPE = "text/plain";
    private static final AwsTestLifecycle awsTestLifecycle = AwsTestLifecycle.create(RxS3ImplIT.class);

    private final S3AsyncClient client = awsTestLifecycle.getS3AsyncClient();
    private final RxS3 rxS3 = new RxS3Impl(client);

    @BeforeAll
    static void beforeClass()
    {
        Hooks.onOperatorDebug();
    }

    @AfterAll
    static void cleanUp()
    {
        awsTestLifecycle.s3Cleanup();
    }

    @Test
    @Disabled //not Checking RxS3Impl class, it's checking if we can do someging with sdk2 client
    void shouldCreateBucket()
    {
        String bucketName = awsTestLifecycle.createBucket();


        StepVerifier.create(
            Mono.fromFuture(client.listBuckets(ListBucketsRequest.builder().build()))
                .map(ListBucketsResponse::buckets)
                .flux()
                .flatMap(Flux::fromIterable)
                .map(Bucket::name)
                .filter(bucketName::equals)
        ).expectNext(bucketName)
            .verifyComplete();

    }

    @Test
    void shouldUploadAndRetrieve()
    {
        String bucketName = awsTestLifecycle.createBucket();
        int byteArraySize = 1024;

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(bucketName, KEY, CONTENT_TYPE)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();

        StepVerifier.create(rxS3.getObject(bucketName, KEY)
        ).consumeNextWith(resp -> assertArrayEquals(bytes, resp))
            .verifyComplete();
    }

    @Test
    void shouldUploadSmallFile()
    {
        String bucketName = awsTestLifecycle.createBucket();
        rxS3.upload(bucketName, KEY, ByteBuffer.wrap("".getBytes()), CONTENT_TYPE).block();

        StepVerifier.create(rxS3.getObject(bucketName, KEY)
        ).consumeNextWith(resp -> assertEquals(resp.length, 0))
            .verifyComplete();

    }

    @Test
    void shouldUploadSmallFileWithoutContentType()
    {
        String bucketName = awsTestLifecycle.createBucket();
        rxS3.upload(bucketName, KEY, ByteBuffer.wrap("".getBytes())).block();

        StepVerifier.create(rxS3.getObject(bucketName, KEY)
        ).consumeNextWith(resp -> assertEquals(resp.length, 0))
            .verifyComplete();

    }


    @Test
    void shouldUploadAndRetrieveAsByteBufferWithoutContentType()
    {
        String bucketName = awsTestLifecycle.createBucket();
        int byteArraySize = 1024;

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(bucketName, KEY)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();

        StepVerifier.create(rxS3.getObject(bucketName, KEY)
        ).consumeNextWith(resp -> assertArrayEquals(bytes, resp))
            .verifyComplete();
    }

    @Test
    void shouldUploadBigFileAsByteBuffer()
    {
        String bucketName = awsTestLifecycle.createBucket();
        int byteArraySize = Integer.MAX_VALUE / 20;
        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);

        StepVerifier.create(rxS3.upload(bucketName, KEY, CONTENT_TYPE).apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();
    }

    @Test
    void shouldListObjects()
    {
        String bucketName = awsTestLifecycle.createBucket();
        int byteArraySize = 1024;

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(bucketName, KEY, CONTENT_TYPE)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();
        StepVerifier.create(rxS3.upload(bucketName, "differentkey", CONTENT_TYPE)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();

        StepVerifier.create(rxS3.listObjects(bucketName, KEY)
        ).expectNext(KEY)
            .verifyComplete();
    }

    @Test
    void shouldDeleteObject()
    {
        String bucketName = awsTestLifecycle.createBucket();
        int byteArraySize = 1024;

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(bucketName, KEY, CONTENT_TYPE).apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();

        StepVerifier.create(rxS3.getObject(bucketName, KEY))
            .consumeNextWith(resp -> assertArrayEquals(bytes, resp))
            .verifyComplete();

        StepVerifier.create(rxS3.deleteObject(bucketName, KEY)).verifyComplete();

        StepVerifier.create(rxS3.getObject(bucketName, KEY))
            .expectError(NoSuchKeyException.class)
            .verify();
    }
}