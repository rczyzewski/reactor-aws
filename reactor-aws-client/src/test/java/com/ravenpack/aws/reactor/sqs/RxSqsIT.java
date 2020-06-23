package com.ravenpack.aws.reactor.sqs;

import com.ravenpack.aws.reactor.Localstack;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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

@Slf4j
@Testcontainers
class RxSqsITTest
{
    @Container
    private static final Localstack localstack = new Localstack()
            .withServices(Localstack.Service.SQS);

    private final SqsAsyncClient sqsClient = localstack.getSqsAsyncClient();
    private final RxSqs rxSqs = RxSqsImpl.builder()
            .client(sqsClient)
            .build();


    @BeforeAll
    static void beforeClass()
    {
        Hooks.onOperatorDebug();
    }

    @AfterAll
    static void cleanUp()
    {
        localstack.sqsCleanup();
    }

    @Test
    void shouldGetQueueUrl()
    {

        String queueName = "queueName";

        String queueUrl = localstack.createSqsQueue(queueName);

        StepVerifier.create(rxSqs.queueUrl(queueName))
                .expectNext(queueUrl)
                .verifyComplete();

    }

    @Test
    void testShouldSuccessfullySendMessagesBatch()
    {

        int numberOfMessages = 3;
        Flux<Message> messages = createMessageFlux(numberOfMessages);

        String queueUrl = localstack.createSqsQueue("queueName");

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
                              Assertions.assertEquals(numberOfMessages, receiveMessageResponse.messages().size()))
            .verifyComplete();
    }

    @Test
    void shouldSuccessfullySendMessagesBatchWithCustomBatchSizeFlatMap()
    {

        int numberOfMessages = 5;
        Flux<Message> messages = createMessageFlux(numberOfMessages);

        String queueUrl = localstack.createSqsQueue("someUrl");

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
                              Assertions.assertEquals(numberOfMessages, receiveMessageResponse.messages().size()))
            .verifyComplete();
    }

    @Test
    void shouldBatchProcess()
    {
        int numberOfMessages = 15;

        Mono<String> queueUrl = Mono.just(localstack.createSqsQueue("someame"));

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
