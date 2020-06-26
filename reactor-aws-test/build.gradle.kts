plugins {
    `java-library`
    id("io.freefair.lombok") version "5.0.1"
}

dependencies {

   api(platform("software.amazon.awssdk:bom:2.13.39"))
   api(platform("org.testcontainers:testcontainers-bom:1.14.3"))
    api(platform("org.junit:junit-bom:5.6.2"))
    api(platform("io.projectreactor:reactor-bom:Dysprosium-SR9"))


    /* These becomes compileTime only, when AwsTestLifecycle pass away */
    api("org.testcontainers:testcontainers")
    api("io.projectreactor:reactor-core")
    api("io.projectreactor.addons:reactor-extra")
    api("software.amazon.awssdk:dynamodb")
    api("software.amazon.awssdk:sqs")
    api("software.amazon.awssdk:s3")
    api("software.amazon.awssdk:lambda")
    api("software.amazon.awssdk:cloudwatchlogs")
    api("software.amazon.awssdk:kinesis")
    api("software.amazon.awssdk:cloudwatch")

    implementation("software.amazon.awssdk:netty-nio-client")
    implementation("software.amazon.awssdk:apache-client")
    implementation("software.amazon.awssdk:http-client-spi")




}
