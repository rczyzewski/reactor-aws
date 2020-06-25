package com.ravenpack.aws.reactor;

import lombok.AllArgsConstructor;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.CreateFunctionRequest;
import software.amazon.awssdk.services.lambda.model.CreateFunctionResponse;
import software.amazon.awssdk.services.lambda.model.DeleteFunctionRequest;
import software.amazon.awssdk.services.lambda.model.Environment;
import software.amazon.awssdk.services.lambda.model.FunctionCode;

@AllArgsConstructor
public class TestHelperLambda {

    private final Localstack localstack;

    public LambdaAsyncClient getLambdaAsyncClient()
    {
        return LambdaAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(Localstack.Service.LAMBDA))
                .credentialsProvider(localstack.getCredentials())
                .region(Region.of(localstack.getRegion()))
                .build();
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
}
