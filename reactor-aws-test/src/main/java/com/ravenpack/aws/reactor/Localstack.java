package com.ravenpack.aws.reactor;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rnorth.ducttape.Preconditions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    /*  This method is taken from https://github.com/testcontainers/testcontainers-java/
        We would like to use all module of localstack, but it requires the old sdk in the runtime.
     */
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
            String endpointURL = "http://" + ipAddress + ":" + this.getMappedPort(service.getPort());
            return new URI(endpointURL);


        } catch (URISyntaxException | UnknownHostException var4) {
            throw new IllegalStateException("Cannot obtain endpoint URL", var4);
        }
    }

    public Localstack withServices(Service... services) {
        this.services.addAll(Arrays.asList(services));
        return this.self();
    }


    public StaticCredentialsProvider getCredentials(){

        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                getAccessKey(), getSecretKey()
        ));
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


    /*  Taken from https://github.com/testcontainers/testcontainers-java/ */
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

    }
}
