package com.ravenpack.aws.reactor;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.utils.AttributeMap;

import static software.amazon.awssdk.core.SdkSystemSetting.CBOR_ENABLED;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

@AllArgsConstructor
public class TestHelperKinesis {

    private final Localstack localstack;

    public KinesisClient getKinesisClient()
    {
        System.setProperty(CBOR_ENABLED.property(), "false");

        SdkHttpClient ddd = ApacheHttpClient
                .builder()
                .buildWithDefaults(
                        AttributeMap
                                .builder()
                                .put(TRUST_ALL_CERTIFICATES, true).build());

        return KinesisClient.builder()
                .httpClient(ddd)
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.KINESIS))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))
                .build();
    }


    public KinesisAsyncClient getKinesisAsyncClient()
    {
        System.setProperty(CBOR_ENABLED.property(), "false");

        SdkAsyncHttpClient ddd =
                NettyNioAsyncHttpClient
                        .builder()
                        .protocol(Protocol.HTTP1_1)
                        .buildWithDefaults(
                                AttributeMap
                                        .builder()
                                        .put(TRUST_ALL_CERTIFICATES, true).build());

        return KinesisAsyncClient.builder()
                .httpClient(ddd)
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.KINESIS))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey()
                )))
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    public CreateStreamResponse createStream(String stream) {
            return getKinesisClient().createStream(
                        CreateStreamRequest.builder()
                                .shardCount(1)
                                .streamName(stream)
                                .build()) ;

    }



}
