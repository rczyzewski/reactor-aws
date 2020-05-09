package com.ravenpack.content.commons.aws.reactor.kinesis;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Builder
public class SchedulerFactory
{

    @NonNull
    private final String streamName;

    @NonNull
    private String appName;

    @NonNull
    private String workIdentifier;

    @NonNull
    private CloudWatchAsyncClient cloudWatchAsyncClient;

    @NonNull
    private KinesisAsyncClient kinesisAsyncClient;

    @NonNull
    private DynamoDbAsyncClient dynamoDbAsyncClient;

    private Function<ConfigsBuilder, CheckpointConfig> checkpointConfig;

    private Function<ConfigsBuilder, CoordinatorConfig> coordinatorConfig;

    private Function<ConfigsBuilder, LeaseManagementConfig> leaseManagementConfig;

    private Function<ConfigsBuilder, LifecycleConfig> lifecycleConfig;

    private Function<ConfigsBuilder, MetricsConfig> metricsConfig;

    private Function<ConfigsBuilder, RetrievalConfig> retrievalConfig;

    public Scheduler createScheduler(Consumer<ProcessRecordsInput> consumer)
    {

        ProcessorConfig processorConfig = new ProcessorConfig(() -> new RecordProcessor(consumer));

        ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName,
                                                           appName,
                                                           this.kinesisAsyncClient,
                                                           this.dynamoDbAsyncClient,
                                                           this.cloudWatchAsyncClient,
                                                           workIdentifier,
                                                           () -> new RecordProcessor(consumer)
        );

        return new Scheduler(
            Optional.ofNullable(checkpointConfig).map(it -> it.apply(configsBuilder))
                .orElseGet(configsBuilder::checkpointConfig),
            Optional.ofNullable(coordinatorConfig).map(it -> it.apply(configsBuilder))
                .orElseGet(configsBuilder::coordinatorConfig),
            Optional.ofNullable(leaseManagementConfig).map(it -> it.apply(configsBuilder))
                .orElseGet(configsBuilder::leaseManagementConfig),
            Optional.ofNullable(lifecycleConfig).map(it -> it.apply(configsBuilder))
                .orElseGet(configsBuilder::lifecycleConfig),
            Optional.ofNullable(metricsConfig).map(it -> it.apply(configsBuilder))
                .orElseGet(configsBuilder::metricsConfig),
            processorConfig,
            Optional.ofNullable(retrievalConfig).map(it -> it.apply(configsBuilder))
                .orElseGet(configsBuilder::retrievalConfig));
    }
}
