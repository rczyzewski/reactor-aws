package com.ravenpack.aws.sample.it;

import com.ravenpack.aws.reactor.Localstack;
import com.ravenpack.aws.reactor.TestHelperDynamoDB;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.reactor.ddb.RxDynamoImpl;
import com.ravenpack.aws.sample.model.ListWithObjectsFieldTable;
import com.ravenpack.aws.sample.model.ListWithObjectsFieldTableRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.ArrayList;

@Slf4j
@Testcontainers
public class ListOfMappedObjectIT {


    @Container
    private static final Localstack localstack =  new Localstack()
            .withServices(Localstack.Service.DDB)
            .withLogConsumer(new Slf4jLogConsumer(log));

    private final TestHelperDynamoDB testHelperDynamoDB = new TestHelperDynamoDB(localstack);

    private  DynamoDbAsyncClient ddbClient = testHelperDynamoDB.getDdbAsyncClient();
    private final RxDynamo rxDynamo  = new RxDynamoImpl(ddbClient);

    @Test
    void testStoringAList(){

        ListWithObjectsFieldTableRepository repo = new ListWithObjectsFieldTableRepository(rxDynamo, "randomTableName");

        rxDynamo.createTable(repo.createTable())
                .block();

        ArrayList<ListWithObjectsFieldTable.InnerObject> a = new ArrayList<>();
        a.add(ListWithObjectsFieldTable.InnerObject.builder().age("2").name("RCZ").build());

        ListWithObjectsFieldTable item = ListWithObjectsFieldTable.builder().uid("myUUID").payload(a).build();
        StepVerifier.create(repo.create(item)).expectNext(item).verifyComplete();

        StepVerifier.create(repo.getAll()).expectNext(item).verifyComplete();

    }


}
