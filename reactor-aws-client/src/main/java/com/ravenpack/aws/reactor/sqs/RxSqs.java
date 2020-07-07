package com.ravenpack.aws.reactor.sqs;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.function.Function;

public interface RxSqs
{
    Mono<String> queueUrl(@NotNull String queueName);

    Flux<SendMessageResponse> send(@NotNull Mono<String> queueUrl, @NonNull String message);

    <T> Flux<Tuple2<T, MessageStatus>> send(
        @NotNull String queueUrl,
        @NotNull Flux<T> messages,
        @NotNull Function<T, String> toString);

    <T> Function<Flux<T>, Flux<Tuple2<T, MessageStatus>>> send(
        @NotNull String queueUrl,
        @NotNull Function<T, String> toString);

     Flux<Message> fetch(@NotNull String queueUrl);

    /** takes messages, when they are available,
     *  finish when no more available.
     */
    Flux<Message> getAll(@NotNull String queueUrl);

    Mono<Message> delete(Message message, Mono<String> queueUrl);

    @NotNull
    Function<Flux<Message>, Flux<DeleteMessageBatchResultEntry>> delete(Mono<String> queueUrl);


}
