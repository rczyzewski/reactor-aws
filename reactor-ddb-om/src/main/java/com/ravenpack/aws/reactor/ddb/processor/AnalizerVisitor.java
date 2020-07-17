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
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleElementVisitor8;

import javax.lang.model.util.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public class AnalizerVisitor extends SimpleElementVisitor8<Object, Map<String, ClassDescription>>  {

    private Logger logger;
    private Types types;


    @SneakyThrows
    ClassDescription generate(Element element)
    {

        HashMap<String,ClassDescription> wc = new HashMap<>();
        element.accept(this ,wc);

        return wc.get(element.getSimpleName().toString());

    }


    public AnalizerVisitor visitType(TypeElement element,  Map<String, ClassDescription> o) {


        String name = element.getSimpleName().toString();
        logger.warn("visitType()" + name );

        if (null != element.getAnnotation(DynamoDBTable.class) ||
                null != element.getAnnotation(DynamoDBDocument.class)) {

            ClassDescription discoveredClass =  ClassDescription.builder()
                    .name(name)
                    .packageName(element.getEnclosingElement().toString())
                    .fieldDescriptions(new ArrayList<>())
                    .sourandingClasses(o)
                    .build();

            if( ! o.containsKey( name )) {
                o.put(name, discoveredClass);

                element.getEnclosedElements()
                        .stream()
                        .filter(it -> ElementKind.FIELD == it.getKind())
                        .forEach(it -> it.accept(this, o));

            }

        }
        return this;
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

    @Override
    public Object visitVariable(VariableElement e,  Map<String, ClassDescription> o) {


        ClassDescription classDescription = o.get(e.getEnclosingElement().getSimpleName().toString() );


        FieldDescription.DDBType ddbType = Arrays.stream(FieldDescription.DDBType.values())
                .filter(it -> it.match(e))
                .findFirst()
                .orElse(FieldDescription.DDBType.OTHER);


        String name = e.getSimpleName().toString() ;

        types.asElement(e.asType()).accept(this, o)  ;
        List<String> typeeArguments  = Collections.emptyList();

        if(  e.asType() instanceof  DeclaredType   )
        {
            for (TypeMirror typeArgument : ((DeclaredType) e.asType()).getTypeArguments()) {
                types.asElement(typeArgument).accept(this, o);
            }

            typeeArguments =    ((DeclaredType) e.asType()).getTypeArguments().stream()
                    .map( types::asElement)
                    .map(it -> it.getSimpleName().toString())
                    .collect(Collectors.toList());
        }


        classDescription.getFieldDescriptions().add(
         FieldDescription.builder()
                .name(name)
                .typeName(e.asType().toString())
                .typePackage(e.asType().toString())
                .ddbType(ddbType)
                 .typeArguments(typeeArguments)
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
                 .sourandingClasses(o)
                 .classReference( ((DeclaredType) e.asType()).asElement().getSimpleName().toString() )
                .attribute(Optional.of(DynamoDBAttribute.class)
                        .map(e::getAnnotation)
                        .map(DynamoDBAttribute::attributeName)
                        .orElse(name))
                .build()
        );

        return this;
    }

    public Object visitTypeParameter(TypeParameterElement e,  Map<String, ClassDescription> o) {
        logger.info(" TypeParameetr:  " + e );
        return true;
    }
}
