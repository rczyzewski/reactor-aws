package com.ravenpack.aws.reactor;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

@AllArgsConstructor
public class TestHelperCloudWatch {

    private final Localstack localstack;

    public CloudWatchAsyncClient getCloudWatchAsyncClient()
    {
        return CloudWatchAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.CLOUD_WATCH))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    public CloudWatchClient getCloudWatchClient()
    {
        return CloudWatchClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.CLOUD_WATCH))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))
                .build();
    }

}
