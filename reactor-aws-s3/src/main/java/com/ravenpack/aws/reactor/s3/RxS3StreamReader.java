package com.ravenpack.aws.reactor.s3;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

@Slf4j
public class RxS3StreamReader
{
    private static final long DEFAULT_CHUNK_SIZE = 1024 * 1024 * 100; //100 MB
    private static final int MAX_RETRIES = 10;
    private static final int BACKOFF_DELAY_MS = 100;
    private static final int CONCURRENCY_LEVEL = 10;

    private final S3AsyncClient s3AsyncClient;

    public RxS3StreamReader(S3AsyncClient s3AsyncClient)
    {
        this.s3AsyncClient = s3AsyncClient;
    }

    public Mono<InputStream> getObject(String bucket, String key)
    {
        return getObject(bucket, key, DEFAULT_CHUNK_SIZE);
    }

    public Mono<InputStream> getObject(String bucket, String key, long chunkSize)
    {
        if (chunkSize <= 0)
            return Mono.error(new IllegalArgumentException("'chunkSize' must be positive"));

        return
            Mono.from(getSize(bucket, key))
                .flatMapMany(size -> Flux.fromStream(getRanges(size, chunkSize).stream()))
                .flatMapSequential(range ->
                                       getPartial(bucket, key, range.getT1(), range.getT2()), CONCURRENCY_LEVEL)
                .collectList()
                .map(l -> new SequenceInputStream(new Vector<>(l).elements()));
    }

    private Mono<ByteArrayInputStream> getPartial(String bucket, String key, long start, long end)
    {
        return
            Mono.just(GetObjectRequest.builder().bucket(bucket).key(key).range(
                    String.format("bytes=%d-%d", start, end)).build())
                .doOnNext(__ -> log.debug("Retrieving {}/{} range:[{}, {}]", bucket, key, start, end))
                .map(req -> s3AsyncClient.getObject(req, AsyncResponseTransformer.toBytes()))
                .flatMap(Mono::fromFuture)
                .map(res -> new ByteArrayInputStream(res.asByteArray()))
                .subscribeOn(Schedulers.elastic())
                .doOnNext(__ -> log.debug("Retrieved {}/{} range:[{}, {}]", bucket, key, start, end))
                .retryWhen(Retry.backoff(MAX_RETRIES, Duration.ofMillis(BACKOFF_DELAY_MS)));
    }

    private List<Tuple2<Long, Long>> getRanges(long size, long chunkSize)
    {
        long current = 0L;
        List<Tuple2<Long, Long>> ranges = new LinkedList<>();
        while (current < size) {
            ranges.add(Tuples.of(current, current + chunkSize - 1));
            current += chunkSize;
        }

        return ranges;
    }

    private Mono<Long> getSize(String bucket, String key)
    {
        return
            Mono.just(HeadObjectRequest.builder().bucket(bucket).key(key).build())
                .map(s3AsyncClient::headObject)
                .flatMap(Mono::fromFuture)
                .map(HeadObjectResponse::contentLength);
    }
}
