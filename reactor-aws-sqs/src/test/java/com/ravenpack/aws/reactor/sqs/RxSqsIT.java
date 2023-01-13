package com.ravenpack.aws.reactor.sqs;

import com.ravenpack.aws.reactor.Localstack;
import com.ravenpack.aws.reactor.TestHelperSqs;
import com.ravenpack.aws.reactor.TestHelpersS3;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;

import static reactor.core.publisher.Hooks.onOperatorDebug;

@Slf4j
@Testcontainers
@Disabled
class RxSqsIT
{
    @Container
    private static final Localstack localstack =  new Localstack()
            .withServices(Localstack.Service.SQS)
            .withLogConsumer(new Slf4jLogConsumer(log));

    private final TestHelperSqs testHelperSqs = new TestHelperSqs(localstack);
    private final TestHelpersS3 testHelpersS3 = new TestHelpersS3(localstack);

    private final S3AsyncClient client = testHelpersS3.getS3AsyncClient();
    private final SqsAsyncClient sqsClient = testHelperSqs.getSqsAsyncClient();
    private final RxSqs rxSqs = RxSqsImpl.builder()
            .client(sqsClient)
            .build();

    private String queueName = "TestQueueName";
    private String queueUrl;

    @BeforeAll
    static void beforeClass()
    {
        onOperatorDebug();
    }

    @BeforeEach
     void prepare()
    {
        queueUrl =  testHelperSqs.createSqsQueue(queueName);
    }
    @AfterEach
    void cleanUp(){
        testHelperSqs.sqsCleanup();
    }

    @Test
    void shouldGetQueueUrl()
    {
        StepVerifier.create(rxSqs.queueUrl(queueName))
                .expectNext(queueUrl)
                .verifyComplete();
    }

    @Test
    void testShouldSuccessfullySendMessagesBatch()
    {

        int numberOfMessages = 3;
        Flux<TestMessage> messages = createMessageFlux(numberOfMessages);

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
        Flux<TestMessage> messages = createMessageFlux(numberOfMessages);

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
    void shouldOperateOnSingleMessages() {
        Mono<String> queueUrlMono = Mono.just(queueUrl);
        StepVerifier.create(
                rxSqs.send(queueUrlMono, "RandomMessage")
                        .map(SendMessageResponse::messageId)
                        .flatMap(it -> rxSqs.fetch(queueUrl))
                        .flatMap(it -> rxSqs.delete(it, queueUrlMono))
        ).expectNextMatches(it -> it.body().contains("RandomMessage"))
                .verifyComplete();
    }

    @Test
    void shouldBatchProcess()
    {
        int numberOfMessages = 15;

        Flux<TestMessage> messages = createMessageFlux(numberOfMessages).delayElements(Duration.ofMillis(100));

        LocalDateTime startSend = LocalDateTime.now();
        StepVerifier.create(
                rxSqs.send(queueUrl, messages, Objects::toString)
                        .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
                .verifyComplete();
        log.info("Sent {} messages in {}ms", numberOfMessages,
                Duration.between(startSend, LocalDateTime.now()).toMillis());

        LocalDateTime start = LocalDateTime.now();
        StepVerifier.create( Flux.just(queueUrl).flatMap(rxSqs::getAll)
                .transform(rxSqs.delete( Mono.just(queueUrl))))
                .expectNextCount(numberOfMessages)
                .verifyComplete();
        log.info("Processed {} messages in {}ms", numberOfMessages,
                Duration.between(start, LocalDateTime.now()).toMillis());
    }

    /***
     * Fetching messages from SQS too much in advance leads to many messages in processing state.
     * When processing of a single message is significant, it's not wise to fetch too many messages,
     * that we won't be able to process in given time.
     * Especially when you have to process messages one by one(because for example it requires a lot of memory).
     */
    @Disabled //Because it's take some time to make this check
    @Test
    void shouldBatchProcessBiggerDataSet()
    {

        //This Flux provide data about messages not Visible
        Flux<Integer> notVisible = Mono.just(GetQueueAttributesRequest.builder().queueUrl(queueUrl).attributeNames(QueueAttributeName.ALL).build())
                .map(sqsClient::getQueueAttributes)
                .flatMap(Mono::fromFuture)
                .map(it -> it.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
                .map(Integer::valueOf)
                .repeat()
                .log("INFLIGHT", Level.INFO, SignalType.ON_NEXT)
                .limitRate(1);

         RxSqs rxSqs = RxSqsImpl.builder()
                .client(sqsClient)
                 .maximumBatchSize(1)
                .build();

        Function<String, Flux<String>> process =
                (String it )-> Flux.just("value" + it)
                        .log("PROCESSING START", Level.INFO, SignalType.ON_NEXT)
                        .delayElements(Duration.ofSeconds(2))
                        .log("PROCESSING END", Level.INFO, SignalType.ON_NEXT);
        int numberOfMessages = 1500;

        Flux<TestMessage> messages = createMessageFlux(numberOfMessages);

        StepVerifier.create(
            this.rxSqs.send(queueUrl, messages, Objects::toString)
                .filter(t -> MessageStatus.SUCCESS.equals(t.getT2()))
        ).expectNextCount(numberOfMessages)
            .verifyComplete();

        log.info("Sent {} messages.", numberOfMessages);

        LocalDateTime start = LocalDateTime.now();

        StepVerifier.create( Flux.just(queueUrl).concatMap(rxSqs::getAll)

                                .concatMap( it -> Flux.just(it)
                                    .map(Message::body)
                                    .concatMap( process )
                                        .ignoreElements()
                                        .thenReturn(it)
                                )
                                .zipWith(notVisible , Tuples::of)
                                .map( it -> {   if( it.getT2()  > 6 ) throw new RuntimeException("Too Much In Flight"); return it.getT1();  })

                                .transform(rxSqs.delete( Mono.just(queueUrl))))
            .expectNextCount(numberOfMessages)
            .verifyComplete();

        log.info("Processed {} messages in {}ms", numberOfMessages,
                 Duration.between(start, LocalDateTime.now()).toMillis());
    }

    @ToString
    @Builder
    public static class TestMessage{
        String id;
        String body;
    }

    public static Flux<TestMessage> createMessageFlux(int numberOfMessages)
    {
        return Flux.range(0, numberOfMessages)
            .map(id -> TestMessage.builder()
                .id(id + "")
                .body(id + " body")
                .build());
    }
}
