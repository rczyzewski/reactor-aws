package com.ravenpack.aws.reactor;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.rnorth.ducttape.Preconditions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;
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
import software.amazon.awssdk.regions.Region;
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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static software.amazon.awssdk.core.SdkSystemSetting.CBOR_ENABLED;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;


@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Localstack extends GenericContainer<Localstack>
{


    static final String VERSION = "0.10.8";
    private static final String HOSTNAME_EXTERNAL_ENV_VAR = "HOSTNAME_EXTERNAL";
    private final List<Service> services;

    public Localstack() {
        this(VERSION);


    }

    public Localstack(String version) {
        super(TestcontainersConfiguration.getInstance().getLocalStackImage() + ":" + version);
        this.services = new ArrayList<>();
        this.withFileSystemBind("//var/run/docker.sock", "/var/run/docker.sock");
        this.waitingFor(Wait.forLogMessage(".*Ready\\.\n", 1));
    }

    protected void configure() {
        super.configure();
        Preconditions.check("services list must not be empty", !this.services.isEmpty());
        this.withEnv("SERVICES", this.services.stream()
                .map(Service::getLocalstackName)
                .collect(Collectors.joining(",")));

        String hostnameExternalReason;

        this.withEnv("HOSTNAME_EXTERNAL", this.getContainerIpAddress());
        hostnameExternalReason = "to match host-routable address for container";

        this.logger().info("{} environment variable set to {} ({})", new Object[]{HOSTNAME_EXTERNAL_ENV_VAR, this.getEnvMap().get(HOSTNAME_EXTERNAL_ENV_VAR), hostnameExternalReason});

        services.stream()
                .map(Service::getPort)
                .forEach(this::addExposedPort);

    }

    public URI getEndpointOverride(Localstack.Service service) {
        try {
            String address = this.getContainerIpAddress();
            String ipAddress = InetAddress.getByName(address).getHostAddress();
            return new URI("http://" + ipAddress + ":" + this.getMappedPort(service.getPort()));
        } catch (URISyntaxException | UnknownHostException var4) {
            throw new IllegalStateException("Cannot obtain endpoint URL", var4);
        }
    }



    public Localstack withServices(Service... services) {
        this.services.addAll(Arrays.asList(services));
        return this.self();
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
                .endpointOverride(getEndpointOverride(Service.CLOUD_WATCH))
                .credentialsProvider(getCredentials())
                .build();
    }

    public CloudWatchClient getCloudWatchClient()
    {
        return CloudWatchClient.builder()
                .endpointOverride(getEndpointOverride(Service.CLOUD_WATCH))
                .credentialsProvider(getCredentials())
                .build();
    }

    public S3AsyncClient getS3AsyncClient()
    {
        return S3AsyncClient.builder()
                .endpointOverride(getEndpointOverride(Service.S3))
                .credentialsProvider(getCredentials())
                .build();
    }

    public S3Client getS3Client()
    {
        return S3Client.builder()
                .endpointOverride(getEndpointOverride(Service.S3))
                .credentialsProvider(getCredentials())
                .build();
    }

    public SqsAsyncClient getSqsAsyncClient()
    {
        return SqsAsyncClient.builder()
                .endpointOverride(getEndpointOverride(Service.SQS))
                .credentialsProvider(getCredentials())
                .build();
    }

    public SqsClient getSqsClient()
    {
        return SqsClient.builder()
                .endpointOverride(getEndpointOverride(Service.SQS))
                .credentialsProvider(getCredentials())
                .build();
    }

    public DynamoDbAsyncClient getDdbAsyncClient()
    {
        return DynamoDbAsyncClient.builder()
                .endpointOverride(getEndpointOverride(Service.DDB))
                .credentialsProvider(getCredentials())
                .build();
    }

    public DynamoDbClient getDdbClient()
    {
        return DynamoDbClient.builder()
                .endpointOverride(getEndpointOverride(Service.DDB))
                .credentialsProvider(getCredentials())
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
                .endpointOverride(getEndpointOverride(Service.KINESIS))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        getAccessKey(), getSecretKey()
                )))
                .region(Region.of(getRegion()))
                .build();
    }
    private StaticCredentialsProvider getCredentials(){

        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                getAccessKey(), getSecretKey()
        ));


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
                .endpointOverride(getEndpointOverride(Service.KINESIS))
                .credentialsProvider(getCredentials())
                .region(Region.of(getRegion()))
                .build();
    }

    public LambdaAsyncClient getLambdaAsyncClient()
    {
        return LambdaAsyncClient.builder()
                .endpointOverride(getEndpointOverride(Service.LAMBDA))
                .credentialsProvider(getCredentials())
                .region(Region.of(getRegion()))
                .build();
    }

    public CloudWatchLogsAsyncClient getCloudWatchLogsAsyncClient()
    {
        return CloudWatchLogsAsyncClient.builder()
                .endpointOverride(getEndpointOverride(Service.LOGS))
                .credentialsProvider(getCredentials())
                .region(Region.of(getRegion()))
                .build();
    }

    public void sqsCleanup()
    {
        Flux.fromIterable(getSqsClient().listQueues().queueUrls())
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
                .flatMap(bucket -> emptyBucket(bucket.name())
                        .flatMap(it -> Mono.fromFuture(
                                getS3AsyncClient()
                                        .deleteBucket(DeleteBucketRequest.builder()
                                                .bucket(bucket.name())
                                                .build()))));
    }

    public void ddbCleanup(String... tableNames)
    {
        Flux.fromArray(tableNames)
                .map(tableName -> getDdbClient().deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))

                .blockLast();
    }

    public void ddbCleanup()
    {
        Flux.fromIterable(getDdbClient().listTables().tableNames())
                .map(tableName -> getDdbClient().deleteTable(DeleteTableRequest.builder().tableName(tableName).build()))
                .blockLast();
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


    public String createBucket(String bucketName)
    {
        getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        return bucketName;
    }

    public String getAccessKey() {
        return "accesskey";
    }

    public String getSecretKey() {
        return "secretkey";
    }

    public String getRegion() {
        return "us-east-1";
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





    @Getter
    @AllArgsConstructor
    public enum Service
    {
        API_GATEWAY("apigateway", 4567),
        KINESIS("kinesis", 4568),
        DDB("dynamodb", 4569),
        DYNAMODB_STREAMS("dynamodbstreams", 4570),
        S3("s3", 4572),
        FIREHOSE("firehose", 4573),
        LAMBDA("lambda", 4574),
        SNS("sns", 4575),
        SQS("sqs", 4576),
        REDSHIFT("redshift", 4577),
        SES("ses", 4579),
        ROUTE53("route53", 4580),
        CLOUDFORMATION("cloudformation", 4581),
        CLOUD_WATCH("cloudwatch", 4582),
        SSM("ssm", 4583),
        SECRETSMANAGER("secretsmanager", 4584),
        STEPFUNCTIONS("stepfunctions", 4585),
        LOGS("logs", 4586),
        STS("sts", 4592),
        IAM("iam", 4593);


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
