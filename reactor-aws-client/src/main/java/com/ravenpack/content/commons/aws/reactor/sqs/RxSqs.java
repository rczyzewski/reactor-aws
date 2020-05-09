package com.ravenpack.content.commons.aws.reactor.sqs;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.function.Function;

public interface RxSqs
{
    Mono<String> queueUrl(@NotNull String queueName);

    <T> Flux<Tuple2<T, MessageStatus>> send(
        @NotNull String queueUrl,
        @NotNull Flux<T> messages,
        @NotNull Function<T, String> toString);

    <T> Function<Flux<T>, Flux<Tuple2<T, MessageStatus>>> send(
        @NotNull String queueUrl,
        @NotNull Function<T, String> toString);

     Flux<Message> fetch(@NotNull String queueUrl);

    Flux<Message> getAll(@NotNull String queueUrl);

    @Contract(pure = true)
    @NotNull
    Function<Flux<Message>, Flux<DeleteMessageBatchResultEntry>> delete(Mono<String> queueUrl);
}
