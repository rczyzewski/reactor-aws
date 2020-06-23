package com.ravenpack.aws.reactor.sqs;

import com.ravenpack.aws.reactor.util.RxUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;

@Slf4j
public class RxSqsImpl implements RxSqs
{
    private final RxSqsSettings settings;

    private final RequestFactory requestFactory;

    private final SqsAsyncClient client;

    @Builder
    private RxSqsImpl(RxSqsSettings settings, SqsAsyncClient client)
    {
        this.settings = settings != null ? settings : RxSqsSettings.create();
        this.client = client;
        this.requestFactory = new RequestFactory(this.settings.getMaximumBatchSize(),
                                                 this.settings.getMaximumBatchWait());
    }

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
    public <T> Flux<Tuple2<T, MessageStatus>> send(
        @NotNull String queueUrl, @NotNull Flux<T> messages, @NotNull Function<T, String> toString)
    {
        return messages.bufferTimeout(settings.getMaximumBatchSize(), settings.getMaximumBatchWait())
            .transform(RxUtils.toMapWithIndex())
                .flatMap(indexedMessages ->
                        requestFactory.createSendMessageBatchRequest(indexedMessages, queueUrl,
                                toString)
                                .map(client::sendMessageBatch)
                                .flatMap(Mono::fromFuture)
                             .log("Messages batch sent", Level.INFO, SignalType.ON_NEXT)
                             .flatMap(response -> convertToMessageStatusTuples(indexedMessages, response))
            );
    }

    private <T> Flux<Tuple2<T, MessageStatus>> convertToMessageStatusTuples(
        Map<Long, T> indexedMessages,
        SendMessageBatchResponse response)
    {
        return getSuccessfullySendMessages(indexedMessages, response)
            .mergeWith(getFailedMessages(indexedMessages, response));
    }

    private <T> Flux<Tuple2<T, MessageStatus>> getSuccessfullySendMessages(
        Map<Long, T> indexedMessages,
        SendMessageBatchResponse response)
    {
        return Flux.fromIterable(response.successful())
            .map(resultEntry -> Tuples.of(indexedMessages.get(Long.valueOf(resultEntry.id())), MessageStatus.SUCCESS));
    }

    private <T> Flux<Tuple2<T, MessageStatus>> getFailedMessages(
        Map<Long, T> indexedMessages,
        SendMessageBatchResponse response)
    {
        return Flux.fromIterable(response.failed())
            .map(
                resultErrorEntry -> Tuples.of(indexedMessages.get(Long.valueOf(resultErrorEntry.id())),
                                              MessageStatus.FAILURE)
            );
    }

    @Override
    public <T> Function<Flux<T>, Flux<Tuple2<T, MessageStatus>>> send(
        @NotNull String queueUrl, @NotNull Function<T, String> toString)
    {
        return f -> send(queueUrl, f, toString);
    }

    @Override
    public Flux<Message> fetch(@NotNull String queueUrl)
    {
        ReceiveMessageRequest req = requestFactory.createReceiveMessageRequest(queueUrl);
        return fetchMessages(req, settings.getMaximumBatchWait().multipliedBy(2) )
        .flatMap(Flux::fromIterable);
    }

    @Override
    public Flux<Message> getAll(@NotNull String queueUrl)
    {

        ReceiveMessageRequest req = requestFactory.createReceiveMessageRequest(queueUrl);

        Flux<Flux<Message>> sourcesOfMessages = Flux.range(0, settings.getParallelism())

            .map(it -> fetchMessages(req, settings.getMaximumBatchWait().multipliedBy(2))
                .expand(list -> {
                    if (list.isEmpty()) {
                        return Flux.empty();
                    } else {
                        return fetchMessages(req, settings.getMaximumBatchWait().multipliedBy(2));
                    }
                })

                .flatMap(Flux::fromIterable))

            .subscribeOn(Schedulers.newSingle("sqs-source"));

        return Flux.merge(sourcesOfMessages, settings.getParallelism());
    }

    @NotNull
    private Flux<List<Message>> fetchMessages(ReceiveMessageRequest request, Duration timeout) {
        return Mono.fromCallable(() -> client.receiveMessage(request))
                .flatMap(Mono::fromFuture)
                .timeout(timeout)
                .log("Fetched messages to process", Level.INFO, SignalType.ON_ERROR, SignalType.ON_NEXT)
                .flux()
                .map(ReceiveMessageResponse::messages);
    }


    public Mono<DeleteMessageResponse> delete(Message message, Mono<String> queueUrl) {

        return queueUrl
                .map(it -> DeleteMessageRequest.builder()
                    .queueUrl(it)
                    .receiptHandle(message.receiptHandle())
                    .build())
                .map(client::deleteMessage)
                .flatMap(Mono::fromFuture)
                .map( it -> it.);
    }


    @Override
    public @NotNull Function<Flux<Message>, Flux<DeleteMessageBatchResultEntry>> delete(Mono<String> queueUrl) {
        return f ->
                f.bufferTimeout(settings.getMaximumBatchSize(), settings.getMaximumBatchWait())
                        .transform(RxUtils::toNewIndex)
                        .withLatestFrom(queueUrl, Tuples::of)
                        .flatMap(indexedMap -> requestFactory.createDeleteMessageBatchRequest(indexedMap.getT1(), indexedMap.getT2()))
                        .log("Batch deleting messages.", Level.FINER, SignalType.ON_NEXT)
                        .flatMap(deleteRequest -> Mono.fromFuture(client.deleteMessageBatch(deleteRequest))
                                .log("Batch delete complete. ", Level.FINER, SignalType.ON_NEXT)
                                .log("Batch delete failed. ", Level.WARNING, SignalType.ON_ERROR)
                                .log()
                                .map(DeleteMessageBatchResponse::successful)
                                .flatMapMany(Flux::fromIterable)
                                .onErrorResume(e -> Mono.empty()));
    }
}