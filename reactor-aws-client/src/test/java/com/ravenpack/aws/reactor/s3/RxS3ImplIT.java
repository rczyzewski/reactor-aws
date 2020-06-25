package com.ravenpack.aws.reactor.s3;

import com.ravenpack.aws.reactor.Localstack;
import com.ravenpack.aws.reactor.TestHelpersS3;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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

@Testcontainers
@Slf4j
class RxS3ImplIT
{
    private static final String KEY = "objectkey";
    private static final String CONTENT_TYPE = "text/plain";



    @Container
    private static final Localstack localstack = new Localstack().withServices(Localstack.Service.DDB,
            Localstack.Service.S3, Localstack.Service.LOGS, Localstack.Service.SQS, Localstack.Service.KINESIS)
            .withLogConsumer(new Slf4jLogConsumer(log));

    private final TestHelpersS3 testHelpersS3 = new TestHelpersS3(localstack);

    private final S3AsyncClient client = testHelpersS3.getS3AsyncClient();
    private final RxS3 rxS3 = new RxS3Impl(client);
    private String bucketName;

    private static final String TEST_BUCKET_NAME = "one.bucket.less";
    @BeforeAll
    static void beforeClass()
    {
        Hooks.onOperatorDebug();
    }

    @AfterEach
     void cleanUp()
    {
        testHelpersS3.s3Cleanup();
    }

    @BeforeEach
    public void  prepare(){
        this.bucketName = testHelpersS3.createBucket(TEST_BUCKET_NAME);
    }

    @Test
    //@Disabled //not Checking RxS3Impl class, it's checking if we can do someging with sdk2 client
    void shouldCreateBucket()
    {

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
        rxS3.upload(bucketName, KEY, ByteBuffer.wrap("".getBytes()), CONTENT_TYPE).block();

        StepVerifier.create(rxS3.getObject(bucketName, KEY)
        ).consumeNextWith(resp -> assertEquals(resp.length, 0))
            .verifyComplete();

    }

    @Test
    void shouldUploadSmallFileWithoutContentType()
    {
        rxS3.upload(bucketName, KEY, ByteBuffer.wrap("".getBytes())).block();

        StepVerifier.create(rxS3.getObject(bucketName, KEY)
        ).consumeNextWith(resp -> assertEquals(resp.length, 0))
            .verifyComplete();

    }


    @Test
    void shouldUploadAndRetrieveAsByteBufferWithoutContentType()
    {
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
        int byteArraySize = Integer.MAX_VALUE / 20;
        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);

        StepVerifier.create(rxS3.upload(bucketName, KEY, CONTENT_TYPE).apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();
    }

    @Test
    void shouldListObjects()
    {
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