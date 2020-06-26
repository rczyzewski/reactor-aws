plugins {
    `maven-publish`
    `java-library`
    id("io.freefair.lombok") version "5.0.1"
}

dependencies {

    api(platform("software.amazon.awssdk:bom:2.13.39"))
    api(platform("org.testcontainers:testcontainers-bom:1.14.3"))
    //https://mvnrepository.com/artifact/org.junit/junit-bom/5.6.2
    api(platform("org.junit:junit-bom:5.6.2"))
    api(platform("io.projectreactor:reactor-bom:Dysprosium-SR9"))


    api("software.amazon.kinesis:amazon-kinesis-client:2.2.3")
    api("software.amazon.awssdk:dynamodb")
    api("software.amazon.awssdk:sqs")
    api("software.amazon.awssdk:s3")
    api("io.projectreactor:reactor-core")
    api("io.projectreactor.addons:reactor-extra")

    compileOnly("org.slf4j:slf4j-api:1.7.28")
    compileOnly("org.jetbrains:annotations:19.0.0")

    testCompileOnly("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

    testImplementation(project(":reactor-aws-test"))
    testImplementation("org.mockito:mockito-junit-jupiter:3.0.0")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.assertj:assertj-core:3.13.2")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("ch.qos.logback:logback-classic:1.2.3")

}