package com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor;

import com.ravenpack.content.commons.aws.reactor.AwsTestLifecycle;
import com.ravenpack.content.commons.aws.reactor.ReactorAWS;
import com.ravenpack.content.commons.aws.reactor.ddb.RxDynamo;
import com.ravenpack.content.commons.aws.sample.CompositePrimaryIndexTable;
import com.ravenpack.content.commons.aws.sample.CompositePrimaryIndexTableRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.UUID;

class UpdateRecordIT
{

    private static final AwsTestLifecycle awsTestLifecycle = AwsTestLifecycle.create(UpdateRecordIT.class);
    private static DynamoDbClient ddbClient = awsTestLifecycle.getDdbClient();

    static {
        Hooks.onOperatorDebug();
    }

    private final RxDynamo rxDynamo = ReactorAWS.dynamo(awsTestLifecycle.getDdbAsyncClient());

    @AfterAll
    static void cleanup()
    {
        awsTestLifecycle.ddbCleanup(getTableNamePrefix());
    }

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
