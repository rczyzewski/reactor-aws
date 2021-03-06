package com.ravenpack.aws.reactor.ddb.mapper;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBDocument;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Slf4j
class LiveMappingDescriptionTest
{

    static <T> List<FieldMappingDescription<T>> createSingleMapping(
        Class<T> ignored,
        BiFunction<T, AttributeValue, T> wither,
        Function<T, Optional<AttributeValue>> export
    )
    {
        return Collections.singletonList(new FieldMappingDescription<>("a", false, wither, export));
    }

    @Test
    void multiplePropertyTest()
    {

        FieldMappingDescription<TestBean> d = new FieldMappingDescription<>("a", true,
            (bean, value) -> bean.withStringProperty(value.s()),
            value -> Optional.of(AttributeValue.builder().s(value.getStringProperty()).build()));

        FieldMappingDescription<TestBean> d2 = new FieldMappingDescription<>("b", true,
            (bean, value) -> bean.withIntegerProperty(Integer.valueOf(value.n())),
            value -> Optional.of(AttributeValue.builder().n(value.getIntegerProperty().toString()).build()));

        FieldMappingDescription<TestBean> d3 = new FieldMappingDescription<>("c", false,
            (bean, value) -> bean.withDoubleProperty(Double.valueOf(value.n())),
            value -> Optional.of(AttributeValue.builder().n(value.getDoubleProperty().toString()).build()));

        FieldMappingDescription<TestBean> d4 = new FieldMappingDescription<>("d", false,
            (bean, value) -> bean.withListStringProperty(value.ss()),
            value -> Optional.of(AttributeValue.builder().ss(value.getListStringProperty()).build()));

        FieldMappingDescription<InternalDocument> internalField = new FieldMappingDescription<>("a", false,
            (bean, value) -> bean.withSomeContentOfInternalDcoument(value.s()),
            value -> Optional.of(AttributeValue.builder().s(value.getSomeContentOfInternalDcoument()).build()));

        LiveMappingDescription<InternalDocument> internal = new LiveMappingDescription<>(InternalDocument::new,
            Collections.singletonList(internalField));

        FieldMappingDescription<TestBean> d5 = new FieldMappingDescription<>("e", false,
            (bean, value) -> bean.withInternalDocument(internal.transform(value.m())),
            value -> Optional.of(AttributeValue.builder().m(internal.export(value.getInternalDocument())).build()));

        LiveMappingDescription<TestBean> dynamoObjectMapper = new LiveMappingDescription<>(TestBean::new,
            Arrays.asList(d, d2, d3, d4, d5));

        TestBean example = TestBean.builder()
            .stringProperty("string")
            .integerProperty(420)
            .doubleProperty(4.20)
            .listStringProperty(Arrays.asList("A", "M", "S"))
            .internalDocument(InternalDocument.builder().someContentOfInternalDcoument("someString").build())
            .build();

        assertThat(dynamoObjectMapper.transform(dynamoObjectMapper.export(example)))
            .isEqualTo(example);

    }

    @Test
    void sunnyDayOnlyWithSimpleString()
    {

        List<FieldMappingDescription<SimpleBeanWithStrings>> mapping = createSingleMapping(SimpleBeanWithStrings.class,
            (bean, value) -> bean.withProperty(value.s()),
            value -> Optional.of(AttributeValue.builder().s(value.getProperty()).build()));

        LiveMappingDescription<SimpleBeanWithStrings> dynamoObjectMapper = new LiveMappingDescription<>(
            SimpleBeanWithStrings::new, mapping);
        SimpleBeanWithStrings example = new SimpleBeanWithStrings("someString");

        StepVerifier.create(Flux.just(example).map(dynamoObjectMapper::export).map(dynamoObjectMapper::transform))
            .expectNext(example)
            .verifyComplete();

    }

    @Test
    void elementNotPresent()
    {

        List<FieldMappingDescription<SimpleBeanWithStrings>> mapping = createSingleMapping(SimpleBeanWithStrings.class,
            (bean, value) -> bean.withProperty(value.s()),
            value -> Optional.of(AttributeValue.builder().s(value.getProperty()).build()));

        LiveMappingDescription<SimpleBeanWithStrings> dynamoObjectMapper = new LiveMappingDescription<>(
            SimpleBeanWithStrings::new, mapping);
        SimpleBeanWithStrings example = new SimpleBeanWithStrings(null);

        StepVerifier.create(Flux.just(example).map(dynamoObjectMapper::export).map(dynamoObjectMapper::transform))
            .expectNext(example)
            .verifyComplete();

    }

    @Test
    void easyIntegerMapping()
    {

        List<FieldMappingDescription<SimpleBeanWithInteger>> mapping =
            createSingleMapping(SimpleBeanWithInteger.class,
                (bean, value) -> bean.withProperty(Integer.valueOf(value.n())),
                null);

        LiveMappingDescription<SimpleBeanWithInteger> dynamoObjectMapper = new LiveMappingDescription<>(
            SimpleBeanWithInteger::new, mapping);

        //I think it caused a lot of pain, when in Dynamo was stored String
        // , that we treated as an Int, and we don't have value and no error

        assertThatThrownBy(
            () -> dynamoObjectMapper.transform(Collections.singletonMap("a", AttributeValue.builder().n("A").build())))
            .isInstanceOf(java.lang.NumberFormatException.class);

        assertThat(
            dynamoObjectMapper.transform(Collections.singletonMap("a", AttributeValue.builder().n("12").build())))
            .matches(it -> it.getProperty().equals(12));

    }

    @Test
    void easyListMapping()
    {

        List<FieldMappingDescription<SimpleBeanWithListOfStrings>> mapping = createSingleMapping(
            SimpleBeanWithListOfStrings.class,
            (bean, value) -> bean.withProperty(value.ss()), null);

        LiveMappingDescription<SimpleBeanWithListOfStrings> dynamoObjectMapper = new LiveMappingDescription<>(
            SimpleBeanWithListOfStrings::new, mapping);

        StepVerifier.create(
            Flux.just(
                dynamoObjectMapper.transform(Collections.singletonMap("a", AttributeValue.builder().ss("12").build()))))
            .expectNextMatches(it -> it.getProperty().contains("12"))
            .verifyComplete();

        StepVerifier.create(
            Flux.just(dynamoObjectMapper.transform(
                Collections.singletonMap("a", AttributeValue.builder().ss("12.1").build()))))
            .expectNextMatches(it -> it.getProperty().equals(Collections.singletonList("12.1")))
            .verifyComplete();
    }

}

@With
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class TestBean
{

    private Integer integerProperty;
    private String stringProperty;
    private Double doubleProperty;
    private List<String> listStringProperty;

    private InternalDocument internalDocument;
}

@NoArgsConstructor
@AllArgsConstructor
@With
@Getter
@DynamoDBDocument
@Builder
@Data
class InternalDocument
{
    private String someContentOfInternalDcoument;
}

@With
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class SimpleBeanWithStrings
{
    private String property;
}

@With
@Data
@NoArgsConstructor
@AllArgsConstructor
class SimpleBeanWithInteger
{
    private Integer property;
}

@With
@Data
@NoArgsConstructor
@AllArgsConstructor
class SimpleBeanWithListOfStrings
{
    private List<String> property;
}

