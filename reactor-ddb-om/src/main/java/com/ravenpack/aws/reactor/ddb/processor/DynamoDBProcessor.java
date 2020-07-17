package com.ravenpack.aws.reactor.ddb.processor;

import com.google.auto.service.AutoService;
import com.ravenpack.aws.reactor.ddb.BaseRepository;
import com.ravenpack.aws.reactor.ddb.DynamoSearch;
import com.ravenpack.aws.reactor.ddb.RxDynamo;
import com.ravenpack.aws.reactor.ddb.datamodeling.DynamoDBTable;
import com.ravenpack.aws.reactor.ddb.mapper.LiveMappingDescription;
import com.ravenpack.aws.reactor.ddb.processor.generator.FilterMethodsCreator;
import com.ravenpack.aws.reactor.ddb.processor.generator.LiveDescriptionGenerator;
import com.ravenpack.aws.reactor.ddb.processor.model.ClassDescription;
import com.ravenpack.aws.reactor.ddb.processor.model.IndexDescription;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import lombok.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Types;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ravenpack.aws.reactor.ddb.processor.TypoUtils.toSnakeCase;
import static com.squareup.javapoet.ParameterizedTypeName.get;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

/***
 * This is the entry point for generating repository, based on model classes
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
@NoArgsConstructor
public class DynamoDBProcessor extends AbstractProcessor
{

    private Filer filer;
    private Logger logger;
    private LiveDescriptionGenerator descriptionGenerator;
    private Types types;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnvironment)
    {
        super.init(processingEnvironment);
        filer = processingEnvironment.getFiler();
        logger = new CompileTimeLogger(processingEnvironment.getMessager());
        logger.info("DDBRepoGenerator initialized");
        descriptionGenerator = new LiveDescriptionGenerator(logger);
        types = processingEnvironment.getTypeUtils();

    }

    @Override
    public boolean process(
        Set<? extends TypeElement> annotations,
        RoundEnvironment roundEnv)
    {

        AnalizerVisitor classAnalyzer = new AnalizerVisitor(logger, types);

        roundEnv.getElementsAnnotatedWith(DynamoDBTable.class)
                .stream()
                .filter(it -> ElementKind.CLASS == it.getKind())
                .forEach(it -> {
                    try {
                        writeToFile(classAnalyzer.generate(it));
                    } catch (Exception e) {

                        String stackTrace = Arrays.stream(e.getStackTrace())
                                .map(line -> line.getClassName() + ":" + line.getMethodName() + ":" + line.getLineNumber())
                                .collect(Collectors.joining("\n"));
                        logger.error(String.format( " %s   while processing the class: %s %n %s " , e.getMessage()  , it.getSimpleName() , stackTrace)) ;
                        throw e;
                    }
                });

        return true;
    }

    @SneakyThrows
    private void writeToFile(ClassDescription classDescription)
    {
        ClassName clazz = ClassName.get(classDescription.getPackageName(), classDescription.getName());

        ClassName repositoryClazz = ClassName.get(classDescription.getPackageName(),
                                                  classDescription.getName() + "Repository");

        String mainMapperName = toSnakeCase(classDescription.getName());

        TypeSpec.Builder navigatorClass = TypeSpec
            .classBuilder(repositoryClazz)
            .addSuperinterface(get(ClassName.get(BaseRepository.class), clazz))
            .addAnnotation(Generated.class)
            .addAnnotation(Getter.class)
            .addAnnotation(AllArgsConstructor.class)
            .addModifiers(PUBLIC)
            .addField(FieldSpec.builder(get(RxDynamo.class), "rxDynamo", FINAL, PRIVATE).build())
            .addField(FieldSpec.builder(get(String.class), "tableName", FINAL, PRIVATE).build())
                .addTypes(classDescription.getSourandingClasses().values()
                        .stream()
                        .map(it -> {
                            var  mapperClass = ClassName.get(repositoryClazz.canonicalName(), it.getName() + "CLASS");
                            var modelClass = ClassName.get(it.getPackageName(), it.getName() );
                            var ptype = get(ClassName.get( LiveMappingDescription.class ), modelClass);

                            return TypeSpec.classBuilder(mapperClass )
                                    .addModifiers(STATIC)
                                    .superclass(ptype)
                                    .addMethod(MethodSpec.constructorBuilder().addCode(descriptionGenerator.createMapper(it)).build())
                                    .build();
                        })
                        .collect(Collectors.toList()))
                .addFields(classDescription.getSourandingClasses().values()
                           .stream()
                           .map(it -> {
                               var newClass = ClassName.get(repositoryClazz.canonicalName(), it.getName() + "CLASS");

                               return FieldSpec.builder(get(ClassName.get(LiveMappingDescription.class),
                                       ClassName.get(it.getPackageName(), it.getName())),
                                       toSnakeCase(it.getName()), Modifier.STATIC, PUBLIC, FINAL)
                                       .initializer(CodeBlock.builder().add("new $T()", newClass ).build())
                                       .build();
                           })
                           .collect(Collectors.toCollection(ArrayDeque::new))
            )
            .addMethod(MethodSpec.methodBuilder("requestUpdate")
                           .addModifiers(PUBLIC)
                           .addParameter(ParameterSpec.builder(clazz, "someName").build())
                           .addCode(CodeBlock.builder()
                                        .indent()
                                        .add(
                                            CodeBlock.builder().indent()
                                                .add(" return $T.builder()\n", UpdateItemRequest.class)
                                                .add(".tableName(tableName)\n")
                                                .add(".attributeUpdates($L.exportUpdate(someName))\n", mainMapperName)
                                                .add(".key($L.exportKeys(someName))", mainMapperName)
                                                .add(".build();")
                                                .unindent()
                                                .build())
                                        .unindent()
                                        .build())

                           .returns(get(UpdateItemRequest.class))
                           .build())
            .addMethod(MethodSpec.methodBuilder("update")
                           .addModifiers(PUBLIC)
                           .addAnnotation(Override.class)
                           .addParameter(ParameterSpec.builder(clazz, "someName").build())
                           .addCode(CodeBlock.builder()
                                        .indent()
                                        .add("return rxDynamo.update(requestUpdate(someName))\n.thenReturn(someName);")
                                        .unindent()
                                        .build())

                           .returns(get(ClassName.get(Mono.class), clazz))
                           .build())
            .addMethod(MethodSpec.methodBuilder("create")
                           .addModifiers(PUBLIC)
                           .addAnnotation(Override.class)
                           .addParameter(ParameterSpec.builder(clazz, "someName").build())
                           .addCode(CodeBlock.builder()
                                        .indent()
                                        .add("return rxDynamo.save(\n$L)\n.thenReturn(someName);",
                                             CodeBlock.builder().indent()
                                                 .add("$T.builder()\n", PutItemRequest.class)
                                                 .add(".tableName(tableName)\n")
                                                 .add(".item($L.export(someName))\n", mainMapperName)
                                                 .add(".build()")
                                                 .unindent()
                                                 .build())
                                        .unindent()
                                        .build())
                           .returns(get(ClassName.get(Mono.class), clazz))
                           .build())
            .addMethod(MethodSpec.methodBuilder("delete")
                           .addModifiers(PUBLIC)
                           .addAnnotation(Override.class)
                           .addParameter(ParameterSpec.builder(clazz, "someName").build())
                           .addCode(CodeBlock.builder().indent()
                                        .add("return rxDynamo.delete(\n$L)\n.then();\n",
                                             CodeBlock.builder().indent()
                                                 .add("$T.builder()\n.key($L.export(someName))\n.build()",
                                                      DeleteItemRequest.class, mainMapperName)
                                                 .unindent()
                                                 .build())
                                        .unindent()
                                        .build())
                           .returns(get(ClassName.get(Mono.class), get(Void.class)))
                           .build())
            .addMethod(MethodSpec.methodBuilder("getAll")
                           .addModifiers(PUBLIC)
                           .addAnnotation(Override.class)
                           .addCode("return rxDynamo.search(\n$L)\n",
                                    CodeBlock.builder().indent().add("$T.builder()\n", DynamoSearch.class)
                                        .add(".tableName(tableName)\n")
                                        .add(".build()\n")
                                        .build())
                           .addCode(".map($L::transform);", mainMapperName)
                           .returns(get(ClassName.get(Flux.class), clazz))
                           .build())
            .addMethod(MethodSpec.methodBuilder("createTable")
                           .addModifiers(PUBLIC)
                           .addAnnotation(Override.class)
                           .addCode(
                               descriptionGenerator.createTableDefinition(new ClassUtils(classDescription, logger)))
                           .returns(ClassName.get(CreateTableRequest.class))
                           .build());

        ClassUtils d = new ClassUtils(classDescription, logger);

        List<IndexDescription> indexes = d.createIndexsDescription();

        indexes.stream()
            .map(it -> MethodSpec.methodBuilder(Optional.of(it)
                                                    .map(IndexDescription::getName)
                                                    .map(TypoUtils::toCamelCase)
                                                    .orElse("primary"))

                .addModifiers(PUBLIC)
                //TODO move class generation for given purpose into a static method
                .returns(ClassName.get(repositoryClazz.packageName(), repositoryClazz.simpleName(),
                        TypoUtils.toClassName(it.getName()) + "CustomSearch"))

                .addCode(
                    "return new $L$L(rxDynamo, DynamoSearch.builder().tableName(tableName).indexName($S).build());\n",
                        TypoUtils.toClassName(it.getName()), "CustomSearch", it.getName())
                .build())
            .forEach(navigatorClass::addMethod);

        indexes.stream().map(it -> fluentQueryGenerator(it, classDescription))
            .forEach(navigatorClass::addType);

        JavaFile.builder(classDescription.getPackageName(), navigatorClass.build())
            .addFileComment("This file is generated by reactive-aws-library")
            .build()
            .writeTo(filer);
    }

    public TypeSpec fluentQueryGenerator(IndexDescription indexDescription, ClassDescription classDescription)
    {

        ClassName clazz = ClassName.get(classDescription.getPackageName(), classDescription.getName());

        ParameterizedTypeName conditionType = get(ClassName.get(Map.class),
                                                  ClassName.get(String.class), ClassName.get(Condition.class));

        ClassName customSearchKF = ClassName.get(classDescription.getPackageName(),
                                                 classDescription.getName() + "Repository",
                TypoUtils.toClassName(indexDescription.getName()) +
                                                 "CustomSearch", "KeyFilterCreator");
        logger.info("customSearchKF" + customSearchKF);

        ClassName customSearchAF = ClassName.get(classDescription.getPackageName(),
                                                 classDescription.getName() + "Repository",
                TypoUtils.toClassName(indexDescription.getName()) +
                                                 "CustomSearch", "FilterCreator");

        logger.info("customSearchAF" + customSearchAF);

        ClassName customSearchCN = ClassName.get(classDescription.getPackageName(),
                                                 classDescription.getName() + "Repository",
                TypoUtils.toClassName(indexDescription.getName()) +
                                                 "CustomSearch");

        logger.info("customSearchCN" + customSearchCN);

        TypeSpec queryClass = TypeSpec.classBuilder(customSearchAF)
            .addModifiers(PUBLIC, FINAL)
            .addAnnotation(With.class)
            .addAnnotation(AllArgsConstructor.class)
            .addField(FieldSpec.builder(customSearchCN, "customSearch", FINAL, PRIVATE).build())
            .addField(FieldSpec.builder(conditionType, "conditionHashMap").build())
            .addMethods(FilterMethodsCreator.createAllFiltersMethod(customSearchAF, indexDescription))
            .addMethod(MethodSpec.methodBuilder("end")
                           .addModifiers(PUBLIC)
                           .returns(customSearchCN)
                           .addCode("return this.customSearch.withDynamoSearch($L);",
                                    CodeBlock.of(
                                        "this.customSearch.dynamoSearch.withFilterConditions(conditionHashMap)"))
                           .build())
            .build();

        TypeSpec keyQueryClass = TypeSpec.classBuilder(customSearchKF)
            .addModifiers(PUBLIC, FINAL)
            .addAnnotation(With.class)
            .addAnnotation(AllArgsConstructor.class)
            .addField(FieldSpec.builder(customSearchCN, "customSearch", FINAL, PRIVATE).build())
            .addField(FieldSpec.builder(conditionType, "conditionHashMap").build())
            .addMethods(FilterMethodsCreator.createKeyFiltersMethod(customSearchKF, indexDescription))
            .addMethod(MethodSpec.methodBuilder("end")
                           .addModifiers(PUBLIC)
                           .returns(customSearchCN)
                           .addCode("return this.customSearch.withDynamoSearch($L);",
                                    CodeBlock.of(
                                        "this.customSearch.dynamoSearch.withKeyConditions(conditionHashMap)"))
                           .build())
            .build();

        return TypeSpec.classBuilder(customSearchCN)
            .addAnnotation(With.class)
            .addAnnotation(Getter.class)
            .addAnnotation(AllArgsConstructor.class)
            .addField(FieldSpec.builder(RxDynamo.class, "rxDynamo", FINAL, PRIVATE).build())
            .addField(FieldSpec.builder(DynamoSearch.class, "dynamoSearch", FINAL, PRIVATE).build())
            .addModifiers(PUBLIC, FINAL)
            .addMethod(MethodSpec.methodBuilder("execute")
                           .addModifiers(PUBLIC)
                           .returns(get(ClassName.get(Flux.class), clazz))
                           .addCode("return rxDynamo.search(dynamoSearch)")
                           .addCode(".map($L::transform);", toSnakeCase(classDescription.getName()))
                           .build())
            .addMethod(MethodSpec.methodBuilder("filter")
                           .addModifiers(PUBLIC)
                           .returns(customSearchAF)
                           .addCode("return new $L(this, $L);\n", customSearchAF,
                                    CodeBlock.of("new $L<>()", ClassName.get(HashMap.class)))
                           .build())
            .addMethod(MethodSpec.methodBuilder("keyFilter")
                           .addModifiers(PUBLIC)
                           .returns(customSearchKF)
                           .addCode("return new $L(this, $L);\n", customSearchKF,
                                    CodeBlock.of("new $L<>()", ClassName.get(HashMap.class)))
                           .build())
            .addType(queryClass)
            .addType(keyQueryClass)
            .build();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes()
    {
        return Collections.singleton(DynamoDBTable.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.RELEASE_8;
    }

}
