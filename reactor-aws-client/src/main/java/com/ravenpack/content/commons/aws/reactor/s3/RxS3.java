package com.ravenpack.content.commons.aws.reactor.s3;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface RxS3
{

    Mono<byte[]> getObject(@NotNull String bucket, @NotNull String key);

    Mono<Void> upload(
        @NotNull String bucket,
        @NotNull String key,
        @NotNull ByteBuffer content);

    Mono<Void> upload(
        @NotNull String bucket,
        @NotNull String key,
        @NotNull ByteBuffer content,
        @NotNull String contentType);

    Function<Flux<ByteBuffer>, Mono<Void>> upload(
        @NotNull String bucket,
        @NotNull String key);

    Function<Flux<ByteBuffer>, Mono<Void>> upload(
        @NotNull String bucket,
        @NotNull String key,
        @NotNull String contentType);

    Flux<String> listObjects(@NotNull String bucket, @NotNull String prefix);

    Mono<Void> deleteObject(@NotNull String bucket, @NotNull String key);
}
