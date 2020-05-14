package com.ravenpack.aws.sample.it;

import com.ravenpack.aws.reactor.ReactorAWS;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.sample.model.CompositePrimaryIndexTable;
import com.ravenpack.aws.sample.model.CompositePrimaryIndexTableRepository;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.UUID;

class UpdateRecordTest
{

    private static DynamoDbAsyncClient ddbClient = TestInfrastrucureHelper.dynamoDbAsyncClient();


    private final RxDynamo rxDynamo = ReactorAWS.dynamo(ddbClient);


    @Test
    void simpleUpdate()
    {
        CompositePrimaryIndexTableRepository repo = new CompositePrimaryIndexTableRepository(rxDynamo, getTableName());

        rxDynamo.createTable(repo.createTable())
            .block();

        CompositePrimaryIndexTable item = CompositePrimaryIndexTable.builder()
            .uid("someUID")
            .payload("ABC")
            .range("A")
            .fuzzyVal(322.0)
            .build();

        repo.create(item).block();

        StepVerifier.create(repo.getAll()).expectNext(item).verifyComplete();

        repo.update(item.withPayload(null)).block();
        StepVerifier.create(repo.getAll()).expectNext(item).verifyComplete();
    }

    private static String getTableNamePrefix(){
        return UpdateRecordTest.class.getSimpleName();
    }

    private String getTableName()
    {
        return getTableNamePrefix() + UUID.randomUUID();
    }
}
