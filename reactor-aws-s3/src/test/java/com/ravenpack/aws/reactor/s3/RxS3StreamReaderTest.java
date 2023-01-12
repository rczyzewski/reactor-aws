package com.ravenpack.aws.reactor.s3;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RxS3StreamReaderTest
{
    private static final String S3_BUCKET = "testBucket";

    private static final String S3_KEY = "testObject";

    @Mock
    private S3AsyncClient client;

    @InjectMocks
    private RxS3StreamReader reader;

    @Test
    @SuppressWarnings("unchecked")
    void happyPathTest()
    {
        byte[] payload = "testPayload".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<HeadObjectResponse> headObjectResponseFuture = CompletableFuture.completedFuture(
            HeadObjectResponse.builder().contentLength((long) payload.length).build());

        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponseFuture);

        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), payload)));

        StepVerifier.create(reader.getObject(S3_BUCKET, S3_KEY))
                    .consumeNextWith(is -> {
                        try {
                            assertArrayEquals(payload, IOUtils.toByteArray(is));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void retriesTest()
    {
        byte[] payload = "testPayload".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<HeadObjectResponse> headObjectResponseFuture = CompletableFuture.completedFuture(
            HeadObjectResponse.builder().contentLength((long) payload.length).build());

        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponseFuture);

        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenThrow(new RuntimeException("Some exception"))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), payload)));

        StepVerifier.create(reader.getObject(S3_BUCKET, S3_KEY))
                    .consumeNextWith(is -> {
                        try {
                            assertArrayEquals(payload, IOUtils.toByteArray(is));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void errorTest()
    {
        byte[] payload = "testPayload".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<HeadObjectResponse> headObjectResponseFuture = CompletableFuture.completedFuture(
            HeadObjectResponse.builder().contentLength((long) payload.length).build());

        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponseFuture);

        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
            .thenThrow(new RuntimeException("Some exception"));

        StepVerifier.withVirtualTime(() -> reader.getObject(S3_BUCKET, S3_KEY))
                    .thenAwait(Duration.ofMinutes(5))
                    .verifyError();
    }

    @Test
    @SuppressWarnings("unchecked")
    void happyPathWithMultipleChunksTest()
    {
        byte[] payload = "testPayload".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<HeadObjectResponse> headObjectResponseFuture = CompletableFuture.completedFuture(
            HeadObjectResponse.builder().contentLength((long) payload.length).build());

        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponseFuture);

        GetObjectRequest getRequestRange1 = GetObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY).range(
            String.format("bytes=%d-%d", 0, 3)).build();

        GetObjectRequest getRequestRange2 = GetObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY).range(
            String.format("bytes=%d-%d", 4, 7)).build();

        GetObjectRequest getRequestRange3 = GetObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY).range(
            String.format("bytes=%d-%d", 8, 11)).build();

        when(client.getObject(eq(getRequestRange1), any(AsyncResponseTransformer.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                            "test".getBytes(StandardCharsets.UTF_8))));

        when(client.getObject(eq(getRequestRange2), any(AsyncResponseTransformer.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                            "Payl".getBytes(StandardCharsets.UTF_8))));

        when(client.getObject(eq(getRequestRange3), any(AsyncResponseTransformer.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                            "oad".getBytes(StandardCharsets.UTF_8))));

        StepVerifier.create(reader.getObject(S3_BUCKET, S3_KEY, 4))
                    .consumeNextWith(is -> {
                        try {
                            assertArrayEquals(payload, IOUtils.toByteArray(is));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).verifyComplete();

    }

    @Test
    @SuppressWarnings("unchecked")
    void happyPathWithMultipleChunksWithRetriesTest()
    {
        byte[] payload = "testPayload".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<HeadObjectResponse> headObjectResponseFuture = CompletableFuture.completedFuture(
            HeadObjectResponse.builder().contentLength((long) payload.length).build());

        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponseFuture);

        GetObjectRequest getRequestRange1 = GetObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY).range(
            String.format("bytes=%d-%d", 0, 3)).build();

        GetObjectRequest getRequestRange2 = GetObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY).range(
            String.format("bytes=%d-%d", 4, 7)).build();

        GetObjectRequest getRequestRange3 = GetObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY).range(
            String.format("bytes=%d-%d", 8, 11)).build();

        when(client.getObject(eq(getRequestRange1), any(AsyncResponseTransformer.class)))
            .thenThrow(new RuntimeException("Some exception"))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                            "test".getBytes(StandardCharsets.UTF_8))));

        when(client.getObject(eq(getRequestRange2), any(AsyncResponseTransformer.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                            "Payl".getBytes(StandardCharsets.UTF_8))));

        when(client.getObject(eq(getRequestRange3), any(AsyncResponseTransformer.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                                            "oad".getBytes(StandardCharsets.UTF_8))));

        StepVerifier.create(reader.getObject(S3_BUCKET, S3_KEY, 4))
                    .consumeNextWith(is -> {
                        try {
                            assertArrayEquals(payload, IOUtils.toByteArray(is));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).verifyComplete();

    }
}