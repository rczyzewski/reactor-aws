package com.ravenpack.aws.reactor.sqs;

import lombok.Builder;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;

@Builder
public class RxSqsImpl implements RxSqs
{
    public static final Logger log = Loggers.getLogger(RxSqsImpl.class);

    @Builder.Default
    int maximumBatchSize = 10;

    @Builder.Default
    Duration maximumBatchWait = Duration.ofSeconds(20);

    private final SqsAsyncClient client;

    @Override
    public Mono<String> queueUrl(@NotNull String queueName)
    {
        return Mono.just(GetQueueUrlRequest
                .builder()
                .queueName(queueName)
                .build())
                .map(client::getQueueUrl)
                .flatMap(Mono::fromFuture)
                .map(GetQueueUrlResponse::queueUrl);
    }

    @Override
    public Flux<SendMessageResponse> send(@NotNull Mono<String> queueUrl, @NonNull String message )
    {
            return queueUrl.map( it -> SendMessageRequest.builder()
                        .queueUrl(it)
                        .messageBody(message)
                        .build())
                    .flux()
                    .map(client::sendMessage)
                    .log(log, Level.WARNING, true , SignalType.ON_ERROR)
                    .flatMap(Mono::fromFuture);
    }

    @Override
    public <T> Flux<Tuple2<T, MessageStatus>> send(
        @NotNull String queueUrl, @NotNull Flux<T> messages, @NotNull Function<T, String> serializer)
    {

        RequestFactory requestFactory  = new RequestFactory( maximumBatchSize, maximumBatchWait);
        return messages
                .bufferTimeout(maximumBatchSize, maximumBatchWait)
                .flatMap(RxSqsImpl::toMapWithIndex)
                .flatMap(indexedMessages ->
                        requestFactory.createSendMessageBatchRequest(indexedMessages, queueUrl, serializer)
                                .map(client::sendMessageBatch)
                                .flatMap(Mono::fromFuture)
                                .log(log, Level.FINER, true, SignalType.ON_NEXT)
                                .log(log, Level.SEVERE, true, SignalType.ON_ERROR)
                             .flatMap(response -> convertToMessageStatusTuples(indexedMessages, response))
            );
    }

    @Override
    public <T> Function<Flux<T>, Flux<Tuple2<T, MessageStatus>>> send(
        @NotNull String queueUrl, @NotNull Function<T, String> serializer)
    {
        return f -> send(queueUrl, f, serializer);
    }

    @Override
    public Flux<Message> fetch(@NotNull String queueUrl) {
        RequestFactory requestFactory = new RequestFactory(maximumBatchSize, maximumBatchWait);

        ReceiveMessageRequest req = requestFactory.createReceiveMessageRequest(queueUrl);
        return fetchMessages(req, maximumBatchWait.multipliedBy(2))
                .limitRate(1)
                .concatMap(Flux::fromIterable);
    }

     @Override
    public Flux<Message> getAll(@NotNull String queueUrl) {

        RequestFactory requestFactory  = new RequestFactory( maximumBatchSize, maximumBatchWait);

        ReceiveMessageRequest req = requestFactory.createReceiveMessageRequest(queueUrl);

        return fetchMessages(req, maximumBatchWait.multipliedBy(2))
                .expand(list -> Optional.of(list)
                        .filter(it -> !list.isEmpty())
                        .map(it -> fetchMessages(req, maximumBatchWait.multipliedBy(2)))
                        .orElseGet(Flux::empty),1)
            .limitRate(1)
            .concatMap(Flux::fromIterable)
            .limitRate(this.maximumBatchSize);
    }

    @NotNull
    private Flux<List<Message>> fetchMessages(ReceiveMessageRequest request, Duration timeout) {
        return Mono.fromCallable(() -> request)
                .map(client::receiveMessage)
                .flatMap(Mono::fromFuture)
                .log(log, Level.FINEST, true, SignalType.ON_NEXT)
                .log(log, Level.SEVERE, true, SignalType.ON_ERROR)
                .flux()
                .limitRate(1)
                .map(ReceiveMessageResponse::messages);
    }

    @Override
    public Mono<Message> delete(Message message, Mono<String> queueUrl) {

        return queueUrl
                .map(it -> DeleteMessageRequest.builder()
                    .queueUrl(it)
                    .receiptHandle(message.receiptHandle())
                    .build())
                .map(client::deleteMessage)
                .flatMap(Mono::fromFuture)
                .thenReturn( message);
    }

    @Override
    public @NotNull Function<Flux<Message>, Flux<DeleteMessageBatchResultEntry>> delete(Mono<String> queueUrl) {

        RequestFactory requestFactory  = new RequestFactory( maximumBatchSize, maximumBatchWait);

        return f ->
                f.bufferTimeout(maximumBatchSize, maximumBatchWait)
                        .flatMap(RxSqsImpl::toMapWithIndex)
                        .withLatestFrom(queueUrl, Tuples::of)
                        .flatMap(indexedMap -> requestFactory.createDeleteMessageBatchRequest(indexedMap.getT1(), indexedMap.getT2()))
                        .log(log, Level.FINER, true, SignalType.ON_NEXT)
                        .flatMap(deleteRequest -> Mono.fromFuture(client.deleteMessageBatch(deleteRequest))
                                .log(log, Level.FINER, true, SignalType.ON_NEXT)
                                .log(log, Level.WARNING, true, SignalType.ON_ERROR)
                                .flatMap(it -> Flux.just(it)
                                        .flatMapIterable(DeleteMessageBatchResponse::failed)
                                        .log(log, Level.WARNING, true, SignalType.ON_NEXT)
                                        .ignoreElements().thenReturn(it))
                                .map(DeleteMessageBatchResponse::successful)
                                .flatMapMany(Flux::fromIterable)
                        );
    }

    private <T> Flux<Tuple2<T, MessageStatus>> convertToMessageStatusTuples(
            Map<Long, T> indexedMessages,
            SendMessageBatchResponse response) {
        Flux<Tuple2<T, MessageStatus>> failures = Flux.fromIterable(response.failed())
                .map(resultErrorEntry -> Tuples.of(indexedMessages.get(Long.valueOf(resultErrorEntry.id())),
                        MessageStatus.FAILURE))
                .log(log, Level.WARNING, true, SignalType.ON_NEXT);
        return Flux.fromIterable(response.successful())
                .map(resultEntry -> Tuples.of(indexedMessages.get(Long.valueOf(resultEntry.id())), MessageStatus.SUCCESS))
                .mergeWith(failures);
    }

    public static <T> Publisher<Map<Long, T>> toMapWithIndex(List<T> f){
        return   Flux.fromIterable(f)
                .index()
                .collectMap(Tuple2::getT1, Tuple2::getT2);
    }

}