package com.ravenpack.content.commons.aws.reactor.kinesis;

import com.ravenpack.content.commons.aws.reactor.AwsTestLifecycle;
import com.ravenpack.content.commons.aws.reactor.kinesis.RxKinesis;
import com.ravenpack.content.commons.aws.reactor.kinesis.SchedulerFactory;
import com.ravenpack.content.commons.aws.reactor.kinesis.WorldConnector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;
import static software.amazon.kinesis.common.InitialPositionInStreamExtended.newInitialPosition;

@Disabled
class KinesisIT
{

    private static final String STREAM = "aKinesisStream";
    private static final AwsTestLifecycle awsTestLifecycle = AwsTestLifecycle.create(KinesisIT.class);
    private final CloudWatchAsyncClient cloudWatchAsyncClient = awsTestLifecycle.getCloudWatchAsyncClient();
    private final KinesisAsyncClient kinesisAsyncClient = awsTestLifecycle.getKinesisAsyncClient();
    private final DynamoDbAsyncClient dynamoDbAsyncClient = awsTestLifecycle.getDdbAsyncClient();

    @BeforeAll
    static void beforeClass()
    {
        Hooks.onOperatorDebug();
    }

    @Test
    void simpleIntegrationTest()
    {

        System.setProperty("aws.cborEnabled", "false");

        CompletableFuture<CreateStreamResponse> createStreamResponseMono = kinesisAsyncClient
            .createStream(
                CreateStreamRequest.builder()
                    .shardCount(1)
                    .streamName(STREAM)
                    .build());

        Mono.fromFuture(createStreamResponseMono).doOnError(Throwable::getCause).log().block();

        Flux.range(0, 100)
            .delayElements(Duration.ofMillis(10))
            .log()
            .flatMap(ignored -> Mono.fromFuture(kinesisAsyncClient
                                                    .describeStream(DescribeStreamRequest.builder().streamName(STREAM)
                                                                        .build()))
                .filter(it -> it.streamDescription().streamStatus().equals(StreamStatus.ACTIVE))).blockFirst();

        int numberOfRecords = 50;
        Flux.range(0, numberOfRecords)
            .map(it -> PutRecordRequest.builder()
                .partitionKey("sth" + it)
                .streamName(STREAM)
                .data(SdkBytes.fromString("test" + it, StandardCharsets.UTF_8))
                .build())
            .map(kinesisAsyncClient::putRecord)
            .flatMap(Mono::fromFuture)
            .map(PutRecordResponse::sequenceNumber)
            .log("PUT")
            .blockLast();

        SchedulerFactory schedulerFactory = SchedulerFactory
            .builder()
            .streamName(STREAM)
            .appName("thisAppName")
            .workIdentifier("externallyProvidedIdentifier")
            .kinesisAsyncClient(kinesisAsyncClient)
            .dynamoDbAsyncClient(dynamoDbAsyncClient)
            .cloudWatchAsyncClient(cloudWatchAsyncClient)
            .retrievalConfig(configsBuilder -> configsBuilder.retrievalConfig()
                .initialPositionInStreamExtended(newInitialPosition(TRIM_HORIZON))
                .retrievalSpecificConfig(
                    new PollingConfig(
                        configsBuilder
                            .streamName(),
                        configsBuilder.kinesisClient())
                )
            )
            .build();

        RxKinesis rxKinesis = RxKinesis.builder()
            .executorService(() -> Executors.newFixedThreadPool(8))
            .schedulerFactory(schedulerFactory)
            .build();

        StepVerifier.create(rxKinesis.kcl(new WorldConnector<>(numberOfRecords * 2, Duration.ofSeconds(1)))
                                .flatMapIterable(ProcessRecordsInput::records)
                                .timeout(Duration.ofSeconds(20)))
            .expectNextCount(numberOfRecords).verifyError();
    }
}
