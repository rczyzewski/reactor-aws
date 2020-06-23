package com.ravenpack.aws.reactor;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.CreateFunctionRequest;
import software.amazon.awssdk.services.lambda.model.CreateFunctionResponse;
import software.amazon.awssdk.services.lambda.model.DeleteFunctionRequest;
import software.amazon.awssdk.services.lambda.model.Environment;
import software.amazon.awssdk.services.lambda.model.FunctionCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static software.amazon.awssdk.core.SdkSystemSetting.CBOR_ENABLED;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

@Slf4j
@Deprecated
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AwsTestLifecycle
{
    private static final String LOCALSTACK = "localstack_1";
    private static final AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create("u1", "p1");
    private static DockerComposeContainer dockerComposeContainer;

    public static synchronized AwsTestLifecycle create(Class<?> testClass)
    {
        if (dockerComposeContainer == null) {
            dockerComposeContainer = initializeContainer();
        }
        return new AwsTestLifecycle(testClass);
    }

    private final Class<?> testClass;

    @SneakyThrows
    private static DockerComposeContainer initializeContainer()
    {

        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(
            "compose-test.yml");
        Path outputFile = Paths.get("target/compose-test.yml");
        Files.deleteIfExists(outputFile);
        Files.copy(Objects.requireNonNull(resourceAsStream), outputFile);

        Map<String, String> env = getEnvVarsMap();
        DockerComposeContainer dockerComposeContainer = new DockerComposeContainer(
            outputFile.toFile()
        ).withEnv(env);

        Arrays.stream(Service.values())
              .forEach(
                  service ->
                      dockerComposeContainer.withExposedService(LOCALSTACK, service.getPort(),
                                                                Wait.forListeningPort()));

        dockerComposeContainer.start();


        Runtime.getRuntime().addShutdownHook(new Thread(dockerComposeContainer::stop));

        return dockerComposeContainer;
    }

    @NotNull
    private static Map<String, String> getEnvVarsMap()
    {
        String services = Stream.of(Service.values())
                                .map(it -> String.format("%s:%s", it.getLocalstackName(), it.getPort()))
                                .collect(Collectors.joining(","));

        return Collections.singletonMap("SERVICES", services);
    }

    public CloudWatchAsyncClient getCloudWatchAsyncClient()
    {
        return CloudWatchAsyncClient.builder()
                                    .endpointOverride(URI.create(getUrl(Service.CLOUD_WATCH)))
                                    .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                                    .build();
    }

    public CloudWatchClient getCloudWatchClient()
    {
        return CloudWatchClient.builder()
                               .endpointOverride(URI.create(getUrl(Service.CLOUD_WATCH)))
                               .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                               .build();
    }

    public S3AsyncClient getS3AsyncClient()
    {
        return S3AsyncClient.builder()
                            .endpointOverride(URI.create(getUrl(Service.S3)))
                            .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                            .build();
    }

    public S3Client getS3Client()
    {
        return S3Client.builder()
                       .endpointOverride(URI.create(getUrl(Service.S3)))
                       .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                       .build();
    }

    public SqsAsyncClient getSqsAsyncClient()
    {
        return SqsAsyncClient.builder()
                             .endpointOverride(URI.create(getUrl(Service.SQS)))
                             .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                             .build();
    }

    public SqsClient getSqsClient()
    {
        return SqsClient.builder()
                        .endpointOverride(URI.create(getUrl(Service.SQS)))
                        .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                        .build();
    }

    public DynamoDbAsyncClient getDdbAsyncClient()
    {
        return DynamoDbAsyncClient.builder()
                                  .endpointOverride(URI.create(getUrl(Service.DDB)))
                                  .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                                  .build();
    }

    public DynamoDbClient getDdbClient()
    {
        return DynamoDbClient.builder()
                             .endpointOverride(URI.create(getUrl(Service.DDB)))
                             .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                             .build();
    }

    public KinesisAsyncClient getKinesisAsyncClient()
    {
        System.setProperty(CBOR_ENABLED.property(), "false");

        SdkAsyncHttpClient ddd =
            NettyNioAsyncHttpClient
                .builder()
                .protocol(Protocol.HTTP1_1)
                .buildWithDefaults(
                    AttributeMap
                        .builder()
                        .put(TRUST_ALL_CERTIFICATES, true).build());

        return KinesisAsyncClient.builder()
                                 .httpClient(ddd)
                                 .endpointOverride(URI.create(getUrl(Service.KINESIS)))
                                 .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                                 .build();
    }

    public KinesisClient getKinesisClient()
    {
        System.setProperty(CBOR_ENABLED.property(), "false");

        SdkHttpClient ddd = ApacheHttpClient
            .builder()
            .buildWithDefaults(
                AttributeMap
                    .builder()
                    .put(TRUST_ALL_CERTIFICATES, true).build());

        return KinesisClient.builder()
                            .httpClient(ddd)
                            .endpointOverride(URI.create(getUrl(Service.KINESIS)))
                            .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                            .build();
    }

    public LambdaAsyncClient getLambdaAsyncClient()
    {
        return LambdaAsyncClient.builder()
                                .endpointOverride(URI.create(getUrl(Service.LAMBDA)))
                                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                                .build();
    }

    public CloudWatchLogsAsyncClient getCloudWatchLogsAsyncClient()
    {
        return CloudWatchLogsAsyncClient.builder()
                                        .endpointOverride(URI.create(getUrl(Service.LOGS)))
                                        .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                                        .build();
    }

    public void sqsCleanup()
    {
        Flux.fromIterable(getSqsClient().listQueues().queueUrls())
            .filter(queueUrl -> queueUrl.startsWith(getResourceNamePrefix()))
            .flatMap(queueUrl -> Mono.fromFuture(
                getSqsAsyncClient().deleteQueue(
                    DeleteQueueRequest.builder()
                                      .queueUrl(queueUrl)
                                      .build())))
            .onErrorContinue((t, o) -> Mono.empty())
            .blockLast();
    }

    public void sqsCleanup(String... queueNames)
    {
        Flux.fromArray(queueNames)
            .flatMap(queueUrl -> Mono.fromFuture(
                getSqsAsyncClient().deleteQueue(
                    DeleteQueueRequest.builder()
                                      .queueUrl(queueUrl)
                                      .build())))
            .onErrorContinue((t, o) -> Mono.empty())
            .blockLast();
    }

    public void s3Cleanup()
    {
        Flux.fromIterable(getS3Client().listBuckets().buckets())
            .filter(bucket -> bucket.name().startsWith(getResourceNamePrefix()))
            .flatMap(bucket -> emptyBucket(bucket.name())
                .flatMap(it -> Mono.fromFuture(
                    getS3AsyncClient()
                        .deleteBucket(DeleteBucketRequest.builder()
                                                         .bucket(bucket.name())
                                                         .build()))))
            .onErrorContinue((t, o) -> Mono.empty())
            .blockLast();
    }

    public void ddbCleanup(String... tableNames)
    {
        Flux.fromArray(tableNames)
            .map(tableName -> getDdbClient().deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))
            .onErrorContinue((t, o) -> Mono.empty())
            .blockLast();
    }

    public void ddbCleanup()
    {
        Flux.fromIterable(getDdbClient().listTables().tableNames())
            .filter(tableName -> tableName.startsWith(getResourceNamePrefix()))
            .map(tableName -> getDdbClient().deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))
            .onErrorContinue((t, o) -> Mono.empty())
            .blockLast();
    }

    /**
     * @return queueUlr
     */
    public String createSqsQueue()
    {
        return createSqsQueue(getResourceName());
    }

    /**
     * @return queueUlr
     */
    public String createSqsQueue(String queueName)
    {
        return getSqsClient().createQueue(CreateQueueRequest.builder().queueName(queueName).build())
                             .queueUrl();
    }

    public void createLogStream(String logGroupName, String streamName)
    {
        CreateLogGroupRequest groupRequest = CreateLogGroupRequest
            .builder().logGroupName(logGroupName).build();
        CreateLogStreamRequest streamRequest = CreateLogStreamRequest.builder()
                                                             .logGroupName(logGroupName)
                                                             .logStreamName(streamName)
                                                             .build();
        Mono
            .fromFuture(getCloudWatchLogsAsyncClient().createLogGroup(groupRequest))
            .flatMap(__ -> Mono.fromFuture(
                getCloudWatchLogsAsyncClient().createLogStream(streamRequest)))
            .block();
    }


    public void deleteLogStream(String logGroupName, String streamName)
    {
        DeleteLogGroupRequest groupRequest = DeleteLogGroupRequest
            .builder().logGroupName(logGroupName).build();
        DeleteLogStreamRequest streamRequest = DeleteLogStreamRequest.builder()
                                                                     .logGroupName(logGroupName)
                                                                     .logStreamName(streamName)
                                                                     .build();
        Mono
            .fromFuture(
                getCloudWatchLogsAsyncClient().deleteLogStream(streamRequest))
            .flatMap(__ -> Mono.fromFuture(
                getCloudWatchLogsAsyncClient().deleteLogGroup(groupRequest)
                ))
            .block();
    }

    public CreateFunctionResponse createLambda(
        String functionName,
        software.amazon.awssdk.services.lambda.model.Runtime runtime,
        byte[] source)
    {
        CreateFunctionRequest req = CreateFunctionRequest
            .builder()
            .functionName(functionName)
            .runtime(runtime)
            .timeout(120)
            .environment(Environment.builder().build())
            .handler("lambda_function.lambda_handler")
            .role("common")
            .code(FunctionCode.builder()
                              .zipFile(SdkBytes.fromByteArray(source))
                              .build())
            .build();
        return Mono.fromFuture(getLambdaAsyncClient().createFunction(req))
                   .block();
    }

    public void removeLambda(String functionName)
    {
        Mono.fromFuture(getLambdaAsyncClient()
                            .deleteFunction(DeleteFunctionRequest.builder()
                                                                 .functionName(functionName)
                                                                 .build()))
            .block();
    }

    public String getSqsQueueName(String queueUrl)
    {
        String[] splittedUrl = queueUrl.split("/");
        return splittedUrl[splittedUrl.length - 1];
    }

    public String createBucket()
    {
        String bucketName = getResourceName();
        getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        return bucketName;
    }

    public String createBucket(String bucketName)
    {
        getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        return bucketName;
    }

    private Mono<Void> emptyBucket(String bucket)
    {
        return Mono.fromFuture(
            getS3AsyncClient().listObjectsV2(ListObjectsV2Request.builder()
                                                                 .bucket(bucket)
                                                                 .build()))
                   .map(ListObjectsV2Response::contents)
                   .flatMapMany(Flux::fromIterable)
                   .map(S3Object::key)
                   .flatMap(key -> Mono.fromFuture(
                       getS3AsyncClient().deleteObject(DeleteObjectRequest.builder()
                                                                          .bucket(bucket)
                                                                          .key(key)
                                                                          .build())))
                   .flatMap(it -> Mono.fromFuture(
                       getS3AsyncClient().listObjectVersions(ListObjectVersionsRequest.builder()
                                                                                      .bucket(bucket).build()))
                                      .map(ListObjectVersionsResponse::versions)
                                      .flatMapMany(Flux::fromIterable)
                                      .flatMap(objectVersion -> Mono.fromFuture(
                                          getS3AsyncClient().deleteObject(DeleteObjectRequest.builder()
                                                                                             .bucket(bucket)
                                                                                             .key(objectVersion.key())
                                                                                             .versionId(
                                                                                                 objectVersion.versionId())
                                                                                             .build())
                                      ))
                   ).onErrorContinue((t, o) -> Mono.empty())
                   .then();
    }

    private String getResourceNamePrefix()
    {
        return testClass.getSimpleName();
    }

    private String getResourceName()
    {
        return (getResourceNamePrefix() + UUID.randomUUID()).toLowerCase();
    }

    @NotNull
    private String getUrl(Service service)
    {
        String ret = "http://" +
                     dockerComposeContainer.getServiceHost(LOCALSTACK, service.getPort()) +
                     ":" +
                     dockerComposeContainer.getServicePort(LOCALSTACK, service.getPort());

        log.info("Client URL {} : {}", service, ret);
        return ret;
    }

    @Getter
    @AllArgsConstructor
    private enum Service
    {
        SQS("sqs", findPort()),
        S3("s3", findPort()),
        DDB("dynamodb", findPort()),
        LAMBDA("lambda", findPort()),
        KINESIS("kinesis", findPort()),
        CLOUD_WATCH("cloudwatch", findPort()),
        LOGS("logs", findPort());
        private final String localstackName;
        private final int port;

        private static int findPort()
        {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                return socket.getLocalPort();
            } catch (IOException e) {
                return findPort();
            }
        }
    }
}
