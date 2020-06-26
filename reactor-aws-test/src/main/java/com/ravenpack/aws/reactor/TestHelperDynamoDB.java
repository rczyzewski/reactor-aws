package com.ravenpack.aws.reactor;

import lombok.AllArgsConstructor;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

@AllArgsConstructor
public class TestHelperDynamoDB {

    private final Localstack localstack;


    public DynamoDbAsyncClient getDdbAsyncClient()
    {
        return DynamoDbAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.DDB))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))

                .build();
    }

    public DynamoDbClient getDdbClient()
    {
        return DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.DDB))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))

                .build();
    }

    public void ddbCleanup(String... tableNames)
    {
        Flux.fromArray(tableNames)
                .map(tableName -> getDdbClient().deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))

                .blockLast();
    }

    public void ddbCleanup()
    {
        Flux.fromIterable(getDdbClient().listTables().tableNames())
                .map(tableName -> getDdbClient().deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))
                .blockLast();
    }

}
