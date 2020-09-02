package com.ravenpack.aws.reactor.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RxS3ImplTest
{

    private static final String BUCKET_NAME = "bucketName";
    private static final String KEY = "objectKey";
    private static final String CONTENT_TYPE = "text/plain";
    private static final String RESULT = "result";

    @Mock
    private S3AsyncClient client;

    @InjectMocks
    private RxS3Impl rxS3;

    @Test
    void shouldDeleteObject()
    {
        when(client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteObjectResponse.builder().build())
        );

        StepVerifier.create(rxS3.deleteObject(BUCKET_NAME, KEY))
            .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldGetObject()
    {
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class))).thenReturn(
            CompletableFuture.completedFuture(
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), RESULT.getBytes()))
        );

        StepVerifier.create(rxS3.getObject(BUCKET_NAME, KEY))
            .consumeNextWith(bytes -> assertEquals(RESULT, new String(bytes, StandardCharsets.UTF_8)))
            .verifyComplete();
    }

    @Test
    void shouldUpload()
    {
        int byteArraySize = 1024;
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder()
                                                              .bucket(BUCKET_NAME)
                                                              .key(KEY)
                                                              .uploadId("uploadId")
                                                              .build()));

        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(UploadPartResponse.builder()
                                                              .eTag("eTag")
                                                              .build()));

        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CompleteMultipartUploadResponse.builder().build()));

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(BUCKET_NAME, KEY, CONTENT_TYPE)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyComplete();
    }

    @Test
    void shouldMergeSmallPiecesTogether()
    {
        int numberOfPieces = 12;
        int pieceSize = Double.valueOf(RxS3Impl.MAX_PART_SIZE / 10f).intValue();
        Flux<ByteBuffer> input = Flux.fromStream(IntStream.range(0, numberOfPieces)
                                                     .boxed()
                                                     .map(it -> {
                                                         byte[] array = new byte[pieceSize];
                                                         new Random().nextBytes(array);
                                                         return ByteBuffer.wrap(array);
                                                     }));

        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder()
                                                              .bucket(BUCKET_NAME)
                                                              .key(KEY)
                                                              .uploadId("uploadId")
                                                              .build()));

        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.completedFuture(UploadPartResponse.builder()
                                                              .eTag("eTag")
                                                              .build()));

        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CompleteMultipartUploadResponse.builder().build()));

        StepVerifier.create(rxS3.upload(BUCKET_NAME, KEY, CONTENT_TYPE).apply(input))
            .verifyComplete();

        ArgumentCaptor<UploadPartRequest> captor = ArgumentCaptor.forClass(UploadPartRequest.class);
        verify(client, times(2)).uploadPart(captor.capture(), any(AsyncRequestBody.class));
        assertEquals(numberOfPieces * pieceSize,
                     captor.getAllValues()
                         .stream()
                         .map(UploadPartRequest::contentLength)
                         .reduce(Long::sum)
                         .get()
                         .intValue());
    }

    @Test
    void shouldAbortMultipartUploadOnError()
    {
        int byteArraySize = 1024;
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder()
                                                              .bucket(BUCKET_NAME)
                                                              .key(KEY)
                                                              .uploadId("uploadId")
                                                              .build()));

        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {throw new RuntimeException();}));

        when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(AbortMultipartUploadResponse.builder().build()));

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(BUCKET_NAME, KEY, CONTENT_TYPE)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyError(RuntimeException.class);

        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void shouldHandleMultipartUploadAbortionError()
    {
        int byteArraySize = 1024;
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CreateMultipartUploadResponse.builder()
                                                              .bucket(BUCKET_NAME)
                                                              .key(KEY)
                                                              .uploadId("uploadId")
                                                              .build()));

        when(client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {throw new RuntimeException();}));

        when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {throw new RuntimeException();}));

        byte[] bytes = new byte[byteArraySize];
        new Random().nextBytes(bytes);
        StepVerifier.create(rxS3.upload(BUCKET_NAME, KEY, CONTENT_TYPE)
                                .apply(Flux.just(ByteBuffer.wrap(bytes))))
            .verifyError(RuntimeException.class);

        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }
}
