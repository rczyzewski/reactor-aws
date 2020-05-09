package com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor.model;

import com.ravenpack.content.commons.aws.reactor.ddb.datamodeling.DynamoDBConverted;
import com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor.generator.NotSupportedTypeException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

import javax.lang.model.element.Element;
import javax.lang.model.type.TypeMirror;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Value
@ToString
@Builder
public class FieldDescription
{
    private final String typeName;
    private final String typePackage;
    private final String name;
    private final String attribute;

    private final TypeMirror conversionClass;

    private final DDBType ddbType;
    private boolean isHashKey;
    private boolean isRangeKey;
    @Builder.Default
    private final List<String> globalIndexRange = Collections.emptyList();
    @Builder.Default
    private final List<String> globalIndexHash = Collections.emptyList();
    private final String localIndex;

    private final ClassDescription classDescription;

    @Getter
    @AllArgsConstructor
    public enum DDBType
    {
        S("s", String.class, true) {
            public boolean match(Element e)
            {
                return "java.lang.String".equals(e.asType().toString());
            }

        },
        C("s", String.class, true) {
            public boolean match(Element e)
            {
                return Optional.ofNullable(e.getAnnotation(DynamoDBConverted.class))
                    .isPresent();
            }
        },
        N("n", Integer.class, false) {
            public boolean match(Element e)
            {
                return "java.lang.Integer".equals(e.asType().toString());
            }

        },
        D("n", Double.class, false) {
            public boolean match(Element e)
            {
                return "java.lang.Double".equals(e.asType().toString());
            }
        },
        L("n", Long.class, false) {
            public boolean match(Element e)
            {
                return "java.lang.Long".equals(e.asType().toString());
            }
        },
        OTHER("UNKNONW", NotSupportedTypeException.class, false) {
            public boolean match(Element e)
            {
                return true;
            }

        };

        private final String symbol;
        private final Class clazz;
        private final boolean listQuerable;

        public abstract boolean match(Element e);
    }

}
