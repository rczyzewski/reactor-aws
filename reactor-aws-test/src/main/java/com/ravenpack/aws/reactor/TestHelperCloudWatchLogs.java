package com.ravenpack.aws.reactor;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;

@Slf4j
@AllArgsConstructor
public class TestHelperCloudWatchLogs {

    private final Localstack localstack;

    public CloudWatchLogsAsyncClient getCloudWatchLogsAsyncClient()
    {
        return CloudWatchLogsAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.LOGS))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))
                .build();
    }


    public void createLogStream(String logGroupName, String streamName)
    {
        CreateLogGroupRequest groupRequest = CreateLogGroupRequest
                .builder().logGroupName(logGroupName).build();
        CreateLogStreamRequest streamRequest = CreateLogStreamRequest.builder()
                .logGroupName(logGroupName)
                .logStreamName(streamName)
                .build();
        Mono
                .fromFuture(getCloudWatchLogsAsyncClient().createLogGroup(groupRequest))
                .flatMap(__ -> Mono.fromFuture(
                        getCloudWatchLogsAsyncClient().createLogStream(streamRequest)))
                .block();
    }


    public void deleteLogStream(String logGroupName, String streamName)
    {
        DeleteLogGroupRequest groupRequest = DeleteLogGroupRequest
                .builder().logGroupName(logGroupName).build();
        DeleteLogStreamRequest streamRequest = DeleteLogStreamRequest.builder()
                .logGroupName(logGroupName)
                .logStreamName(streamName)
                .build();
        Mono
                .fromFuture(
                        getCloudWatchLogsAsyncClient().deleteLogStream(streamRequest))
                .flatMap(__ -> Mono.fromFuture(
                        getCloudWatchLogsAsyncClient().deleteLogGroup(groupRequest)
                ))
                .block();
    }
}
