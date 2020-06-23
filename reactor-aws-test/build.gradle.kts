plugins {
    `java-library`
    id("io.freefair.lombok") version "5.0.1"
}

dependencies {

    implementation(platform("software.amazon.awssdk:bom:2.13.39"))
    implementation(platform("org.testcontainers:testcontainers-bom:1.14.3")) //import bom

    //compile("io.projectreactor:reactor-test:3.3.5.RELEASE")
    api("org.testcontainers:testcontainers")
    implementation( "org.testcontainers:localstack")
    api("io.projectreactor:reactor-core:3.3.5.RELEASE")
    api("io.projectreactor.addons:reactor-extra:3.3.3.RELEASE")
    api("software.amazon.awssdk:dynamodb:2.13.8")
    api("software.amazon.awssdk:sqs:2.13.8")
    api("software.amazon.awssdk:s3:2.13.8")
    api("software.amazon.awssdk:lambda:2.13.8")
    api("software.amazon.awssdk:cloudwatchlogs:2.13.8")
    api("software.amazon.awssdk:apache-client:2.13.8")
    api("software.amazon.awssdk:kinesis:2.13.8")
    api("software.amazon.awssdk:netty-nio-client:2.13.8")
    api("software.amazon.awssdk:http-client-spi:2.13.8")
    implementation("software.amazon.awssdk:cloudwatch:2.13.8")

}
