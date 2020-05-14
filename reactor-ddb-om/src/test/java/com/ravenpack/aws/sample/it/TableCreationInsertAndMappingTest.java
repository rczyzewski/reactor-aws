package com.ravenpack.aws.sample.it;

import com.ravenpack.aws.reactor.ReactorAWS;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.sample.model.GlobalHashIndexTable;
import com.ravenpack.aws.sample.model.GlobalHashIndexTableRepository;
import com.ravenpack.aws.sample.model.GlobalRangeIndexTable;
import com.ravenpack.aws.sample.model.GlobalRangeIndexTableRepository;
import com.ravenpack.aws.sample.model.HashPrimaryIndexTable;
import com.ravenpack.aws.sample.model.HashPrimaryIndexTableRepository;
import com.ravenpack.aws.sample.model.IntegerAsCompositeIndex;
import com.ravenpack.aws.sample.model.IntegerAsCompositeIndexRepository;
import com.ravenpack.aws.sample.model.InternalDocumentTable;
import com.ravenpack.aws.sample.model.InternalDocumentTableRepository;
import com.ravenpack.aws.sample.model.LocalSecondaryIndexTable;
import com.ravenpack.aws.sample.model.LocalSecondaryIndexTableRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.UUID;

class TableCreationInsertAndMappingTest
{

    private final DynamoDbAsyncClient dynamoDbAsyncClient = TestInfrastrucureHelper.dynamoDbAsyncClient();


    private final RxDynamo rxDynamo = ReactorAWS.dynamo(dynamoDbAsyncClient);

    @AfterAll
    static void cleanup()
    {
        //I guess, It might be reasonable to drop tablles that have been created
    }

    @Test
    void simpleHashPrimaryIndex()
    {
        Hooks.onOperatorDebug();
        HashPrimaryIndexTableRepository repo = new HashPrimaryIndexTableRepository(rxDynamo, getTableName());

        HashPrimaryIndexTable item = HashPrimaryIndexTable
            .builder()
            .uid("someUID")
            .payload("payload")
            .build();



        StepVerifier.create(
            rxDynamo.createTable(repo.createTable()).ignoreElement()
                .thenReturn(item)
                .flatMap(repo::create)
                .ignoreElement()
                .thenReturn(repo)
                .flatMapMany(HashPrimaryIndexTableRepository::getAll))
            .expectNext(item)
            .verifyComplete();

        HashPrimaryIndexTable newItem = item.withPayload("newPayload");

        StepVerifier.create(
            repo.update(newItem)
                .thenReturn(repo)
                .flatMapMany(HashPrimaryIndexTableRepository::getAll))
            .expectNext(newItem)
            .verifyComplete();

        HashPrimaryIndexTable newNullItem = item.withPayload(null);

        StepVerifier.create(
            repo.update(newNullItem)
                .thenReturn(repo)
                .flatMapMany(HashPrimaryIndexTableRepository::getAll))
            .expectNext(newItem) // Must not override existing value with null
            .verifyComplete();

    }

    @Test
    void localSecondaryIndexTableRepository()
    {

        LocalSecondaryIndexTableRepository repo = new LocalSecondaryIndexTableRepository(rxDynamo, getTableName());

        LocalSecondaryIndexTable item = LocalSecondaryIndexTable
            .builder()
            .uid("someUID")
            .range("A")
            .secondRange("B")
            .payload("payload")
            .build();

        StepVerifier.create(
            rxDynamo.createTable(repo.createTable()).ignoreElement()
                .thenReturn(item)
                .flatMap(repo::create)
                .ignoreElement()
                .thenReturn(repo)
                .flatMapMany(LocalSecondaryIndexTableRepository::getAll))
            .expectNext(item)
            .verifyComplete();
    }

    @Test
    void globalHashIndexTableRepository()
    {

        GlobalHashIndexTableRepository repo = new GlobalHashIndexTableRepository(rxDynamo, getTableName());

        GlobalHashIndexTable item = GlobalHashIndexTable
            .builder()
            .uid("someUID")
            .globalId("otherId")
            .payload("payload")
            .build();

        StepVerifier.create(
            rxDynamo.createTable(repo.createTable()).ignoreElement()
                .thenReturn(item)
                .flatMap(repo::create)
                .ignoreElement()
                .thenReturn(repo)
                .flatMapMany(GlobalHashIndexTableRepository::getAll))
            .expectNext(item)
            .verifyComplete();
    }

    @Test
    void globalRangeIndexTableRepository()
    {

        GlobalRangeIndexTableRepository repo = new GlobalRangeIndexTableRepository(rxDynamo, getTableName());

        GlobalRangeIndexTable item = GlobalRangeIndexTable
            .builder()
            .uid("someUID")
            .globalId("otherId")
            .globalRange("someRange")
            .payload("payload")
            .build();

        StepVerifier.create(
            rxDynamo.createTable(repo.createTable()).ignoreElement()
                .thenReturn(item)
                .flatMap(repo::create)
                .ignoreElement()
                .thenReturn(repo)
                .flatMapMany(GlobalRangeIndexTableRepository::getAll))
            .expectNext(item)
            .verifyComplete();
    }

    @Test
    void integerAsCompositeIndexRepository()
    {

        IntegerAsCompositeIndexRepository repo = new IntegerAsCompositeIndexRepository(rxDynamo, getTableName());

        IntegerAsCompositeIndex item = IntegerAsCompositeIndex
            .builder()
            .uid(12)
            .range(33)
            .payload("payload")
            .fuzzyVal(23.0)
            .val(12)
            .build();

        StepVerifier.create(
            rxDynamo.createTable(repo.createTable()).ignoreElement()
                .thenReturn(item)
                .flatMap(repo::create)
                .ignoreElement()
                .thenReturn(repo)
                .flatMapMany(IntegerAsCompositeIndexRepository::getAll))
            .expectNext(item)
            .verifyComplete();
    }

    @Test
    void internalDocumentTableRepository()
    {

        InternalDocumentTableRepository repo = new InternalDocumentTableRepository(rxDynamo, getTableName());

        InternalDocumentTable item = InternalDocumentTable
            .builder()
            .uid("uid")
            .payload("someString")
            .content(InternalDocumentTable.InternalDocumentContent.builder().payload("internalPayload").build())

            .build();

        StepVerifier.create(
            rxDynamo.createTable(repo.createTable()).ignoreElement()
                .thenReturn(item)
                .flatMap(repo::create)
                .ignoreElement()
                .thenReturn(repo)
                .flatMapMany(InternalDocumentTableRepository::getAll))
            .expectNext(item)
            .verifyComplete();
    }

    private static String getTableNamePrefix(){
        return TableCreationInsertAndMappingTest.class.getSimpleName();
    }

    private String getTableName()
    {
        return getTableNamePrefix() + UUID.randomUUID();
    }
}
