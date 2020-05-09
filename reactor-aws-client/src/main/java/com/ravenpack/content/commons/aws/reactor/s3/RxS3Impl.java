package com.ravenpack.content.commons.aws.reactor.s3;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;

@Slf4j
@RequiredArgsConstructor
public class RxS3Impl implements RxS3
{

    // Maximum part size in bytes, used to split into multipart upload if needed.
    public static final int MAX_PART_SIZE = 10 * 1024 * 1024;
    private final S3AsyncClient client;

    private static boolean smallPartsBufferPredicate(AtomicInteger bufferedSize, ByteBuffer buffer)
    {
        if (bufferedSize.addAndGet(buffer.remaining()) >= MAX_PART_SIZE) {
            bufferedSize.set(0);
            return true;
        }
        return false;
    }

    @NotNull
    private static Optional<ByteBuffer> reduceListOfByteBuffers(List<ByteBuffer> list)
    {
        return list.stream()
            .reduce((soFar, buffer) ->
                        ByteBuffer.allocate(soFar.capacity() + buffer.capacity())
                            .put(soFar).put(buffer));
    }

    //TODO retrieve metadata
    @Override
    public Mono<byte[]> getObject(@NotNull String bucket, @NotNull String key)
    {
        log.debug("Fetching object as byte array from S3 bucket: {}, key: {}", bucket, key);
        return  Mono.just(GetObjectRequest.builder().bucket(bucket).key(key).build())
                .map( it -> client.getObject(it, AsyncResponseTransformer.toBytes()))
                .flatMap( Mono::fromFuture)
                .map(BytesWrapper::asByteArray);
    }

    @Override
    public Flux<String> listObjects(@NotNull String bucket, @NotNull String prefix)
    {
        log.info("Listing object in S3 bucket: {}", bucket);

        return Flux.from(client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                                                           .bucket(bucket)
                                                           .prefix(prefix)
                                                           .build()))
            .flatMap(response -> Flux.fromIterable(response.contents()))
            .map(S3Object::key);
    }

    @Override
    public Mono<Void> deleteObject(@NotNull String bucket, @NotNull String key)
    {
        log.info("Deleting S3 object bucket: {}, key: {}", bucket, key);

        return Mono.just(DeleteObjectRequest.builder().bucket(bucket).key(key).build())
                .map(client::deleteObject)
                .flatMap(Mono::fromFuture)
                .then();
    }

    @Override
    public Mono<Void> upload(@NotNull String bucket, @NotNull String key, @NotNull ByteBuffer content)
    {
        return Mono.just(PutObjectRequest.builder()
                             .bucket(bucket)
                             .key(key)
                             .serverSideEncryption(ServerSideEncryption.AES256)
                             .build())
            .map(it -> client.putObject(it, AsyncRequestBody.fromByteBuffer(content)))
            .flatMap(Mono::fromFuture)
            .then();
    }


    @Override
    public Mono<Void> upload(
        @NotNull String bucket, @NotNull String key, @NotNull ByteBuffer content,
        @NotNull String contentType)
    {

        return Mono.just(PutObjectRequest.builder()
                             .bucket(bucket)
                             .key(key)
                             .contentType(contentType)
                             .serverSideEncryption(ServerSideEncryption.AES256)
                             .build())
            .map(it -> client.putObject(it, AsyncRequestBody.fromByteBuffer(content)))
            .flatMap(Mono::fromFuture)
            .then();
    }

    @Override
    public Function<Flux<ByteBuffer>, Mono<Void>> upload(
        @NotNull String bucket, @NotNull String key)
    {
        return flux -> Mono.just(RequestFactory.createMultipartUploadRequest(bucket, key))
            .map(client::createMultipartUpload)
            .flatMap(Mono::fromFuture)
            .log("Starting multipart upload", Level.INFO, SignalType.ON_NEXT)
            .flatMap(
                uploadParts(flux)).then();
    }


    public Function<Flux<ByteBuffer>, Mono<Void>> upload2(
            @NotNull String bucket, long length, @NotNull String key)
    {

        return flux-> Mono.just(
        PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .serverSideEncryption(ServerSideEncryption.AES256)
                .contentLength(length)
                .build())
            .map(it -> client.putObject(it, AsyncRequestBody.fromPublisher(flux)))
            .flatMap(Mono::fromFuture)
            .then();
    }

    @Override
    public Function<Flux<ByteBuffer>, Mono<Void>> upload(
        @NotNull String bucket, @NotNull String key,
        @NotNull String contentType)
    {
        return flux -> Mono.just(RequestFactory.createMultipartUploadRequest(bucket, key, contentType))
            .map(client::createMultipartUpload)
            .flatMap(Mono::fromFuture)
            .log("Starting multipart upload", Level.INFO, SignalType.ON_NEXT)
            .flatMap(
                uploadParts(flux)).then();
    }

    @NotNull
    private Function<CreateMultipartUploadResponse, Mono<? extends CompleteMultipartUploadResponse>> uploadParts(Flux<ByteBuffer> flux)
    {
        return (CreateMultipartUploadResponse upload) -> {
            AtomicInteger partNumber = new AtomicInteger(1);
            AtomicInteger bufferedSize = new AtomicInteger(0);
            return flux.bufferUntil(buffer -> smallPartsBufferPredicate(bufferedSize, buffer), true)
                .map(RxS3Impl::reduceListOfByteBuffers)
                .filter(Optional::isPresent)
                .map(buffer -> new PartUploadTupleWithBuffer((ByteBuffer) buffer.get().rewind()))
                .expand(bufferTuple -> prepareAndUploadPart(bufferTuple, upload, partNumber))
                .filter(PartUploadTupleWithBuffer::hasUploadResponse)
                .map(PartUploadTupleWithBuffer::toPartUploadTuple)
                .map(RequestFactory::prepareCompletePart)
                .collectList()
                .map(completedParts -> CompletedMultipartUpload.builder().parts(completedParts).build())
                .map(response -> RequestFactory.prepareCompleteMultipartUploadRequest(upload, response))
                .flatMap(request -> Mono.fromFuture(client.completeMultipartUpload(request)))
                .log("Finished multipart upload", Level.INFO, SignalType.ON_NEXT)
                .doOnError(abortMultipartUpload(upload));
        };
    }

    private Flux<PartUploadTupleWithBuffer> prepareAndUploadPart(
        PartUploadTupleWithBuffer bufferTuple,
        CreateMultipartUploadResponse uploadResponse, AtomicInteger partNumber)
    {
        ByteBuffer buffer = bufferTuple.getByteBuffer();
        int partSize = Math.min(MAX_PART_SIZE, buffer.remaining());
        if (partSize == 0) {
            return Flux.empty();
        }
        byte[] part = new byte[partSize];
        ByteBuffer rest = buffer.get(part, 0, partSize).slice();
        int currentPartNumber = partNumber.getAndIncrement();
        return uploadPart(uploadResponse, part, currentPartNumber)
            .flatMapMany(
                resp -> Mono.just(new PartUploadTupleWithBuffer(rest, currentPartNumber, resp)));
    }

    private Mono<UploadPartResponse> uploadPart(
        CreateMultipartUploadResponse upload, byte[] bytes,
        int partNumber)
    {
        return Mono.fromFuture(
            client.uploadPart(RequestFactory.createUploadPartRequest(upload, bytes, partNumber),
                              AsyncRequestBody.fromBytes(bytes)));
    }

    private Consumer<Throwable> abortMultipartUpload(CreateMultipartUploadResponse upload)
    {
        log.info("Aborting multipart upload {}", upload);
        return throwable ->
            Mono.fromFuture(client.abortMultipartUpload(
                RequestFactory.createAbortMultipartUploadRequest(upload)))
                .log("Multipart upload aborted successfully {}", Level.INFO, SignalType.ON_NEXT)
                .doOnError(error -> log.error("Failed to abort multipart upload {}", upload));
    }

    @Getter
    @RequiredArgsConstructor
    private static final class PartUploadTupleWithBuffer
    {

        private final ByteBuffer byteBuffer;
        private final int partNumber;
        private final UploadPartResponse maybePartUploadResponse;

        private PartUploadTupleWithBuffer(ByteBuffer byteBuffer)
        {
            this.byteBuffer = byteBuffer;
            this.partNumber = 0;
            this.maybePartUploadResponse = null;
        }

        private boolean hasUploadResponse()
        {
            return maybePartUploadResponse != null;
        }

        private PartUploadTuple toPartUploadTuple()
        {
            return new PartUploadTuple(partNumber, maybePartUploadResponse);
        }
    }

    @Getter
    @RequiredArgsConstructor
    static final class PartUploadTuple
    {
        private final int partNumber;
        private final UploadPartResponse partUploadResponse;
    }
}
