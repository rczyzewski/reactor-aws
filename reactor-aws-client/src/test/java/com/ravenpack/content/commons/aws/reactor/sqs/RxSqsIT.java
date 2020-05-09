package com.ravenpack.content.commons.aws.reactor.sqs;

import com.ravenpack.content.commons.aws.reactor.AwsTestLifecycle;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class RxSqsIT
{
    private static final AwsTestLifecycle awsTestLifecycle = AwsTestLifecycle.create(RxSqsIT.class);
    private final SqsAsyncClient sqsClient = awsTestLifecycle.getSqsAsyncClient();
    private final RxSqs rxSqs = RxSqsImpl.builder()
        .client(sqsClient)
        .settings(RxSqsSettings.builder()
                      .maximumBatchSize(10)
                      .maximumBatchWait(Duration.ofSeconds(1))
                      .build()
        ).build();

    @BeforeAll
    static void beforeClass()
    {
        Hooks.onOperatorDebug();
    }

    @AfterAll
    static void cleanUp()
    {
        awsTestLifecycle.sqsCleanup();
    }

    @Test
    void shouldGetQueueUrl()
    {
        String queueUrl = awsTestLifecycle.createSqsQueue();
        String queueName = awsTestLifecycle.getSqsQueueName(queueUrl);

        StepVerifier.create(rxSqs.queueUrl(queueName))
            .expectNext(queueUrl)
            .verifyComplete();
    }

    @Test
    void shouldSuccessfullySendMessagesBatch()
    {
        int numberOfMessages = 3;
        Flux<Message> messages = createMessageFlux(numberOfMessages);

        String queueUrl = awsTestLifecycle.createSqsQueue();

        StepVerifier.create(rxSqs.send(queueUrl, messages, Objects::toString)
                                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
            .verifyComplete();

        StepVerifier.create(
            Mono.fromFuture(
                sqsClient.receiveMessage(
                    ReceiveMessageRequest
                        .builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(numberOfMessages)
                        .waitTimeSeconds(10)
                        .build()))
        ).consumeNextWith(receiveMessageResponse ->
                              assertEquals(numberOfMessages, receiveMessageResponse.messages().size()))
            .verifyComplete();
    }

    @Test
    void shouldSuccessfullySendMessagesBatchWithCustomBatchSizeFlatMap()
    {
        int numberOfMessages = 5;
        Flux<Message> messages = createMessageFlux(numberOfMessages);

        String queueUrl = awsTestLifecycle.createSqsQueue();

        StepVerifier.create(
            messages.window(numberOfMessages)
                .flatMap(
                    messagesFlux ->
                        rxSqs.send(queueUrl, messagesFlux, Objects::toString))
                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
            .verifyComplete();

        StepVerifier.create(
            Mono.fromFuture(
                sqsClient.receiveMessage(
                    ReceiveMessageRequest
                        .builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(numberOfMessages)
                        .waitTimeSeconds(10)
                        .build()))
        ).consumeNextWith(receiveMessageResponse ->
                              assertEquals(numberOfMessages, receiveMessageResponse.messages().size()))
            .verifyComplete();
    }

    @Test
    void shouldBatchProcess()
    {
        int numberOfMessages = 15;

        Mono<String> queueUrl = Mono.just(awsTestLifecycle.createSqsQueue());

        Flux<Message> messages = createMessageFlux(numberOfMessages).delayElements(Duration.ofMillis(100));

        LocalDateTime startSend = LocalDateTime.now();
        StepVerifier.create(
            rxSqs.send(queueUrl.block(), messages, Objects::toString)
                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
            .verifyComplete();
        log.info("Sent {} messages in {}ms", numberOfMessages,
                 Duration.between(startSend, LocalDateTime.now()).toMillis());

        LocalDateTime start = LocalDateTime.now();
        StepVerifier.create( queueUrl.flatMapMany(rxSqs::getAll)
                                .transform(rxSqs.delete(queueUrl)))
            .expectNextCount(numberOfMessages)
            .verifyComplete();
        log.info("Processed {} messages in {}ms", numberOfMessages,
                 Duration.between(start, LocalDateTime.now()).toMillis());
    }

    private Flux<Message> createMessageFlux(int numberOfMessages)
    {
        return Flux.range(0, numberOfMessages)
            .map(id -> Message.builder()
                .messageId(id + "")
                .body(id + " body")
                .build());
    }
}
