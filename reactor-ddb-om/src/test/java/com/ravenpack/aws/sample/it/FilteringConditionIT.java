package com.ravenpack.aws.sample.it;

import com.ravenpack.aws.reactor.Localstack;
import com.ravenpack.aws.reactor.ReactorAWS;
import com.ravenpack.aws.reactor.TestHelperDynamoDB;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.sample.model.CompositePrimaryIndexTable;
import com.ravenpack.aws.sample.model.CompositePrimaryIndexTableRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

@Slf4j
@Testcontainers
class FilteringConditionIT
{
    @Container
    private static final Localstack localstack =  new Localstack()
            .withServices(Localstack.Service.DDB)
            .withLogConsumer(new Slf4jLogConsumer(log));

    private final TestHelperDynamoDB testHelperDynamoDB = new TestHelperDynamoDB(localstack);

    private  DynamoDbAsyncClient ddbClient = testHelperDynamoDB.getDdbAsyncClient();
    private final RxDynamo rxDynamo = ReactorAWS.dynamo(ddbClient);


    private CompositePrimaryIndexTableRepository repo1;

    @AfterAll
    static void cleanup()
    {
    }

    @BeforeEach
    void prepareTable()
    {
        repo1 = new CompositePrimaryIndexTableRepository(rxDynamo, getTableName());

        rxDynamo.createTable(repo1.createTable()).block();

        repo1.create(CompositePrimaryIndexTable.builder()
                         .uid("someUUID")
                         .range("other")
                         .payload("payload")
                         .val(123)
                         .fuzzyVal(4.20)
                         .build())
            .block();
    }

    @Test
    void fullScanTest()
    {
        StepVerifier.create(repo1.getAll().count()).expectNext(1L).verifyComplete();
        StepVerifier.create(repo1.primary().execute().count()).expectNext(1L).verifyComplete();
    }

    private void checkFilter(
        long expected,
        Function<CompositePrimaryIndexTableRepository.nullCustomSearch, CompositePrimaryIndexTableRepository.nullCustomSearch> f)
    {


        StepVerifier.create(
            Flux.just(repo1.primary())
                .flatMap(it -> f.apply(it).execute())
                .count())
            .expectNext(expected)
            .verifyComplete();
    }

    @Test
    void doubleEqualsTest()
    {

        //Equals - I understand why comparing doubles is bad, but for sake of compatibility with dynamo
        checkFilter(1, it -> it.filter().fuzzyValEquals(4.20).end());
        checkFilter(0, it -> it.filter().fuzzyValEquals(20.4).end());
        checkFilter(0, it -> it.filter().fuzzyValEquals(1.0).end());
    }

    @Test
    void stringEqualsTest()
    {

        //Equals
        checkFilter(1, it -> it.filter().payloadEquals("payload").end());
        checkFilter(0, it -> it.filter().payloadEquals("apayload").end());
    }

    @Test
    void integerEqualsTest()
    {

        checkFilter(1, it -> it.filter().valEquals(123).end());
        checkFilter(0, it -> it.filter().valEquals(4).end());
    }

    @Test
    void stringNotEqualTest()
    {
        //NotEquals
        checkFilter(0, it -> it.filter().payloadNotEquals("payload").end());
        checkFilter(1, it -> it.filter().payloadNotEquals("apayload").end());
    }

    @Test
    void integerNotEqualTest()
    {

        checkFilter(0, it -> it.filter().valNotEquals(123).end());
        checkFilter(1, it -> it.filter().valNotEquals(12).end());
    }

    @Test
    void stringInTest()
    {

        //In
        checkFilter(1, it -> it.filter().payloadIn(Collections.singletonList("payload")).end());
        checkFilter(0, it -> it.filter().payloadIn(Collections.singletonList("apayload")).end());
    }

    @Test
    void intInTest()
    {

        //In
        checkFilter(1, it -> it.filter().valIn(Collections.singletonList(123)).end());
        checkFilter(0, it -> it.filter().valIn(Collections.singletonList(12)).end());
    }

    @Test
    void stringLessOrEqual()
    {

        //lessOrEqual
        checkFilter(1, it -> it.filter().payloadLessOrEqual("payload").end());
        checkFilter(1, it -> it.filter().payloadLessOrEqual("payloada").end());
        checkFilter(0, it -> it.filter().payloadLessOrEqual("a").end());
        checkFilter(1, it -> it.filter().payloadLessOrEqual("z").end());
    }

    @Test
    void intLessOrEqual()
    {

        //lessOrEqual
        checkFilter(1, it -> it.filter().valLessOrEqual(123).end());
        checkFilter(1, it -> it.filter().valLessOrEqual(124).end());
        checkFilter(0, it -> it.filter().valLessOrEqual(122).end());

    }

    @Test
    void stringLessThan()
    {

        //lessThan
        checkFilter(0, it -> it.filter().payloadLessThen("payload").end());
        checkFilter(1, it -> it.filter().payloadLessThen("payloada").end());
        checkFilter(0, it -> it.filter().payloadLessThen("a").end());
    }

    @Test
    void intLessThan()
    {
        //lessThan
        checkFilter(1, it -> it.filter().valLessThen(124).end());
        checkFilter(0, it -> it.filter().valLessThen(122).end());
        checkFilter(0, it -> it.filter().valLessThen(123).end());
    }

    @Test
    void stringGraterOrEqual()
    {

        //graterThan
        checkFilter(1, it -> it.filter().payloadGraterOrEquals("payload").end());
        checkFilter(1, it -> it.filter().payloadGraterOrEquals("payloa").end());
        checkFilter(1, it -> it.filter().payloadGraterOrEquals("a").end());
        checkFilter(0, it -> it.filter().payloadGraterOrEquals("z").end());
    }

    @Test
    void intGraterOrEqual()
    {

        //graterThan
        checkFilter(1, it -> it.filter().valGraterOrEquals(123).end());
        checkFilter(1, it -> it.filter().valGraterOrEquals(12).end());
        checkFilter(0, it -> it.filter().valGraterOrEquals(420).end());
    }

    @Test
    void stringGraterThan()
    {

        //graterThan
        checkFilter(0, it -> it.filter().payloadLessThen("payload").end());
        checkFilter(1, it -> it.filter().payloadLessThen("payloada").end());
        checkFilter(0, it -> it.filter().payloadLessThen("a").end());
    }

    @Test
    void intGraterThan()
    {

        checkFilter(0, it -> it.filter().valGraterThan(123).end());
        checkFilter(0, it -> it.filter().valGraterThan(124).end());
        checkFilter(1, it -> it.filter().valGraterThan(121).end());
    }

    @Test
    void stringBeginsWith()
    {

        checkFilter(1, it -> it.filter().payloadBeginsWith("pay").end());
        checkFilter(0, it -> it.filter().payloadBeginsWith("aaapay").end());
    }

    @Test
    void stringIsNull()
    {
        checkFilter(0, it -> it.filter().payloadIsNull().end());
    }

    @Test
    void intIsNull()
    {
        checkFilter(0, it -> it.filter().valIsNull().end());
    }

    @Test
    void stringIsNotNull()
    {
        checkFilter(1, it -> it.filter().payloadIsNotNull().end());
    }

    @Test
    void intIsNotNull()
    {
        checkFilter(1, it -> it.filter().valIsNotNull().end());
    }

    @Test
    void contains()
    {
        checkFilter(1, it -> it.filter().payloadContains("pay").end());
        checkFilter(0, it -> it.filter().payloadContains("ppay").end());
    }

    @Test
    void notContains()
    {
        checkFilter(0, it -> it.filter().payloadNotContains("pay").end());
        checkFilter(1, it -> it.filter().payloadNotContains("notpaid").end());
    }

    @Test
    void stringBetween()
    {

        checkFilter(1, it -> it.filter().payloadBetween("p", "r").end());
        checkFilter(0, it -> it.filter().payloadBetween("a", "b").end());
    }

    @Test
    void intBetween()
    {

        checkFilter(1, it -> it.filter().valBetween(2, 421).end());
        checkFilter(0, it -> it.filter().valBetween(2, 20).end());
    }

    private static String getTableNamePrefix(){
        return FilteringConditionIT.class.getSimpleName();
    }

    private String getTableName()
    {
        return getTableNamePrefix() + UUID.randomUUID();
    }
}
