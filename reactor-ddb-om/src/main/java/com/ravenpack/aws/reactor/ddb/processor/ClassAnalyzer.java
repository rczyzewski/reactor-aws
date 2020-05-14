package com.ravenpack.aws.reactor.ddb.processor;

import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBAttribute;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBConverted;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBDocument;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBIndexHashKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBIndexRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBLocalIndexRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBRangeKey;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import com.ravenpack.aws.reactor.ddb.processor.model.ClassDescription;
import com.ravenpack.aws.reactor.ddb.processor.model.FieldDescription;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ClassAnalyzer
{

    private Logger logger;

    private Types types;

    /* now we are lazy, and classes containing itself are not now supported
     * it's possible, but not a priority right now - there is so much other things that can go bad
     * */

    @SneakyThrows
    ClassDescription generate(Element element)
    {

        if (null != element.getAnnotation(DynamoDBTable.class) ||
            null != element.getAnnotation(DynamoDBDocument.class)) {

            List<FieldDescription> fields = element.getEnclosedElements()
                .stream()
                .filter(it -> ElementKind.FIELD == it.getKind())
                .map(this::analyze)
                .collect(Collectors.toList());

            return ClassDescription.builder()
                .name(element.getSimpleName().toString())
                .packageName(element.getEnclosingElement().toString())
                .fieldDescriptions(fields)
                .build();
        } else if (null != types.asElement(element.asType()).getAnnotation(DynamoDBDocument.class)) {
            return generate(types.asElement(element.asType()));
        } else {
            return null;
        }
    }

    public TypeMirror getConverterMirror(Element e)
    {

        try {
            Optional.ofNullable(e.getAnnotation(DynamoDBConverted.class))
                .ifPresent(DynamoDBConverted::converter);

        } catch (MirroredTypeException ex) {
            return ex.getTypeMirror();
        }
        return null;
    }

    public FieldDescription analyze(Element e)
    {
        String name = e.getSimpleName().toString();

        FieldDescription.DDBType ddbType = Arrays.stream(FieldDescription.DDBType.values())
            .filter(it -> it.match(e))
            .findFirst()
            .orElse(FieldDescription.DDBType.OTHER);

        return FieldDescription.builder()
            .name(name)
            .typeName(e.asType().toString())
            .typePackage(e.asType().toString())
            .ddbType(ddbType)
            .conversionClass(getConverterMirror(e))
            .isHashKey(Optional.ofNullable(e.getAnnotation(DynamoDBHashKey.class)).isPresent())
            .isRangeKey(Optional.ofNullable(e.getAnnotation(DynamoDBRangeKey.class)).isPresent())
            .localIndex(Optional.ofNullable(e.getAnnotation(DynamoDBLocalIndexRangeKey.class))
                            .map(DynamoDBLocalIndexRangeKey::localSecondaryIndexName)
                            .orElse(null))
            .globalIndexHash(Optional.ofNullable(e.getAnnotation(DynamoDBIndexHashKey.class))
                                 .map(DynamoDBIndexHashKey::globalSecondaryIndexNames)
                                 .map(Arrays::asList)
                                 .orElseGet(Collections::emptyList))
            .globalIndexRange(Optional.ofNullable(e.getAnnotation(DynamoDBIndexRangeKey.class))
                                  .map(DynamoDBIndexRangeKey::globalSecondaryIndexNames)
                                  .map(Arrays::asList)
                                  .orElseGet(Collections::emptyList))
            .classDescription(this.generate(e))
            .attribute(Optional.of(DynamoDBAttribute.class)
                           .map(e::getAnnotation)
                           .map(DynamoDBAttribute::attributeName)
                           .orElse(name))
            .build();
    }
}

