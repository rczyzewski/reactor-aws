package com.ravenpack.aws.sample.it;

import com.ravenpack.aws.reactor.Localstack;
import com.ravenpack.aws.reactor.ReactorAWS;
import com.ravenpack.aws.reactor.TestHelperDynamoDB;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.sample.model.CompositePrimaryIndexTable;
import com.ravenpack.aws.sample.model.CompositePrimaryIndexTableRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.UUID;

@Slf4j
@Testcontainers
class UpdateRecordIT
{
    @Container
     private static final Localstack localstack =  new Localstack()
                .withServices(Localstack.Service.DDB)
                .withLogConsumer(new Slf4jLogConsumer(log));

     private final TestHelperDynamoDB testHelperDynamoDB = new TestHelperDynamoDB(localstack);

    private  DynamoDbAsyncClient ddbClient = testHelperDynamoDB.getDdbAsyncClient();

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
        return UpdateRecordIT.class.getSimpleName();
    }

    private String getTableName()
    {
        return getTableNamePrefix() + UUID.randomUUID();
    }
}
