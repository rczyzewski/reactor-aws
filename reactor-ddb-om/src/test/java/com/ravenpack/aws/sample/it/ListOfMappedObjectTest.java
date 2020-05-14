package com.ravenpack.aws.sample.it;

import com.ravenpack.aws.reactor.ReactorAWS;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.sample.model.ListWithObjectsFieldTable;
import com.ravenpack.aws.sample.model.ListWithObjectsFieldTableRepository;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.ArrayList;

public class ListOfMappedObjectTest {

    private static DynamoDbAsyncClient ddbClient = TestInfrastrucureHelper.dynamoDbAsyncClient();


    private final RxDynamo rxDynamo = ReactorAWS.dynamo(ddbClient);

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
