package com.ravenpack.aws.sample.it;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.testcontainers.containers.localstack.LocalStackContainer;
import reactor.core.publisher.Hooks;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestInfrastrucureHelper {

    private static final LocalStackContainer localstack = new LocalStackContainer()
            .withServices(DYNAMODB);


    static {
        Hooks.onOperatorDebug();
        localstack.start();
    }



    public static DynamoDbAsyncClient dynamoDbAsyncClient(){

        return DynamoDbAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(DYNAMODB))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey()
                )))
                .region(Region.of(localstack.getRegion()))
                .build();
    }
}
