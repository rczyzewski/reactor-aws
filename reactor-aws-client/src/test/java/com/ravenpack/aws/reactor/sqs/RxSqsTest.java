package com.ravenpack.aws.reactor.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RxSqsTest
{
    private static final String QUEUE_NAME = "queueName";
    private static final String QUEUE_URL = "http://queueUrl";

    @Mock
    private SqsAsyncClient client;

    private RxSqsImpl rxSqs;

    @BeforeEach
    void cleanup()
    {
        reset(client);
        rxSqs = RxSqsImpl.builder()
            .client(client)
            .settings(RxSqsSettings.builder()
                          .maximumBatchWait(Duration.ofSeconds(1))
                          .parallelism(1)
                          .build())
            .build();
    }

    @Test
    void shouldGetQueueUrl()
    {
        when(client.getQueueUrl(eq(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build())))
                .thenReturn(CompletableFuture.completedFuture(GetQueueUrlResponse
                        .builder()
                        .queueUrl(QUEUE_URL)
                        .build()));

        StepVerifier.create(rxSqs.queueUrl(QUEUE_NAME))
            .expectNext(QUEUE_URL)
            .verifyComplete();
    }

    @Test
    void shouldSuccessfullySendMessagesBatch()
    {
        int numberOfMessages = 3;
        List<Message> messages = createMessages(numberOfMessages);

        when(client.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(prepareSuccessfulSendMessageBatchResponse(messages));

        StepVerifier.create(rxSqs.send(QUEUE_NAME, Flux.fromIterable(messages), Objects::toString)
                                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
            .verifyComplete();
    }

    @Test
    void shouldSuccessfullySendMessagesBatchUsingTransform()
    {
        int numberOfMessages = 3;
        List<Message> messages = createMessages(numberOfMessages);

        when(client.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(prepareSuccessfulSendMessageBatchResponse(messages));

        StepVerifier.create(Flux.fromIterable(messages).transform(rxSqs.send(QUEUE_NAME, Objects::toString))
                                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
            .verifyComplete();
    }

    @Test
    void shouldHandleEmptyBatchToSend()
    {
        Flux<Message> messages = Flux.empty();

        StepVerifier.create(rxSqs.send(QUEUE_NAME, messages, Objects::toString)
                                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).thenAwait(Duration.ofSeconds(2))
            .verifyComplete();
    }

    @Test
    void shouldHandleMessagesWIthDelayBiggerThanBatchMaxWait()
    {
        int numberOfMessages = 5;
        List<Message> messages = createMessages(numberOfMessages);

        doReturn(prepareSuccessfulSendMessageBatchResponse(messages))
            .when(client).sendMessageBatch(any(SendMessageBatchRequest.class));

        StepVerifier.create(rxSqs.send(QUEUE_NAME, Flux.fromIterable(messages)
                                           .delayElements(Duration.ofMillis(2L)),
                                       Objects::toString)
                                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
                                .buffer(3)
                                .flatMap(Flux::fromIterable)
        )
            .thenAwait(Duration.ofMillis(3L * numberOfMessages))
            .expectNextCount(numberOfMessages)
            .verifyComplete();
    }

    @Test
    void shouldSuccessfullySendPartOfMessagesBatch()
    {
        int numberOfSuccessfulMessages = 3;
        List<Message> successfulMessages = createMessageList(numberOfSuccessfulMessages);

        Message failedMessage = Message.builder().messageId(successfulMessages.size() + "").build();
        List<Message> messages = new ArrayList<>(successfulMessages);
        messages.add(failedMessage);

        when(client.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(
                prepareMixedSendMessageBatchResponse(successfulMessages, Collections.singletonList(failedMessage)));

        Flux<Tuple2<Message, MessageStatus>> result = rxSqs.send(QUEUE_NAME, Flux.fromIterable(messages),
                                                                 Objects::toString);
        StepVerifier.create(result)
            .expectNextCount(messages.size())
            .verifyComplete();

        StepVerifier.create(
            result.filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfSuccessfulMessages)
            .verifyComplete();

        StepVerifier.create(
            result.filter(t -> MessageStatus.FAILURE.equals(t.getT2()))
        ).expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void shouldFailToSendMessagesBatch()
    {
        int numberOfMessages = 4;
        List<Message> messages = createMessages(numberOfMessages);

        when(client.sendMessageBatch(any(SendMessageBatchRequest.class)))
            .thenReturn(prepareFailedSendMessageBatchResponse(messages));

        StepVerifier.create(
            rxSqs.send(QUEUE_NAME, Flux.fromIterable(messages), Objects::toString)
                .filter(t -> MessageStatus.FAILURE.equals(t.getT2()))
        )
            .expectNextCount(numberOfMessages)
            .verifyComplete();
    }

    @Test
    void shouldBatchProcess()
    {

        int numberOfMessages = 15;

        when(client.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                ReceiveMessageResponse
                    .builder()
                    .messages(createReceivedMessageList(numberOfMessages))
                    .build())).thenReturn(CompletableFuture.completedFuture(
            ReceiveMessageResponse
                .builder()
                .messages(Collections.emptyList())
                .build()));

        StepVerifier.create( Flux.just(QUEUE_URL).flatMap(rxSqs::getAll)
                                .log())
            .expectNextCount(numberOfMessages)
            .verifyComplete();

        verify(client, times(0)).getQueueUrl(any(GetQueueUrlRequest.class));
        verify(client, times(2)).receiveMessage(any(ReceiveMessageRequest.class));
        // should be called 2 times, when number of messages is bigger than max batch size = 10
        verify(client, times(0)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    private List<Message> createMessageList(int numberOfMessages)
    {
        return IntStream.range(0, numberOfMessages)
            .boxed()
            .map(id -> Message.builder()
                .messageId(id + "")
                .build())
            .collect(Collectors.toList());
    }

    private List<Message> createReceivedMessageList(int numberOfMessages)
    {
        return IntStream.range(0, numberOfMessages)
            .boxed()
            .map(id -> Message.builder()
                .messageId(id + "")
                .body(id + " body")
                .receiptHandle("receiptHandle" + id)
                .build())
            .collect(Collectors.toList());
    }

    private CompletableFuture<SendMessageBatchResponse> prepareSuccessfulSendMessageBatchResponse(List<Message> messages)
    {
        return prepareMixedSendMessageBatchResponse(messages, Collections.emptyList());
    }

    private CompletableFuture<SendMessageBatchResponse> prepareFailedSendMessageBatchResponse(List<Message> messages)
    {
        return prepareMixedSendMessageBatchResponse(Collections.emptyList(), messages);
    }

    private CompletableFuture<SendMessageBatchResponse> prepareMixedSendMessageBatchResponse(
        List<Message> successfulMessages,
        List<Message> failedMessages)
    {

        List<SendMessageBatchResultEntry> successfulEntries = new ArrayList<>(successfulMessages.size());
        for (int i = 0; i < successfulMessages.size(); i++) {
            Message message = successfulMessages.get(i);
            successfulEntries.add(
                SendMessageBatchResultEntry
                    .builder()
                    .id(i + "")
                    .messageId(message.messageId())
                    .build()
            );
        }

        List<BatchResultErrorEntry> failedEntries = new ArrayList<>(failedMessages.size());
        for (int i = 0; i < failedMessages.size(); i++) {
            failedEntries.add(
                BatchResultErrorEntry
                    .builder()
                    .id(i + "")
                    .build()
            );
        }

        return CompletableFuture.completedFuture(SendMessageBatchResponse
                                                     .builder()
                                                     .successful(successfulEntries)
                                                     .failed(failedEntries)
                                                     .build()
        );
    }

    private List<Message> createMessages(int numberOfMessages)
    {
        return IntStream.range(0, numberOfMessages).boxed()
            .map(this::createMessage)
            .collect(Collectors.toList());
    }

    private Message createMessage(long id)
    {
        return Message.builder()
            .messageId(id + "")
            .body(id + " body")
            .build();
    }
}
