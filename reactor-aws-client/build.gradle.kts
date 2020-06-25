plugins {
    `maven-publish`
    `java-library`
    id("io.freefair.lombok") version "5.0.1"
}


dependencies {
    compileOnly("software.amazon.kinesis:amazon-kinesis-client:2.2.3")
    compileOnly("software.amazon.awssdk:dynamodb:2.13.8")
    compileOnly("software.amazon.awssdk:sqs:2.13.8")
    compileOnly("software.amazon.awssdk:s3:2.13.8")
    compileOnly("org.slf4j:slf4j-api:1.7.28")
    compile("org.jetbrains:annotations:17.0.0")



    testCompileOnly("org.junit.jupiter:junit-jupiter-api:5.6.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.2")

    testImplementation(project(":reactor-aws-test"))
    testImplementation("org.mockito:mockito-junit-jupiter:3.0.0")
    testImplementation("io.projectreactor:reactor-test:3.3.5.RELEASE")
    testImplementation("org.assertj:assertj-core:3.13.2")
    
    testImplementation("org.testcontainers:junit-jupiter:1.14.3")

    testImplementation("ch.qos.logback:logback-classic:1.2.3")

    testImplementation("software.amazon.kinesis:amazon-kinesis-client:2.2.3")

    compileOnly("io.projectreactor:reactor-core:3.3.5.RELEASE")
    compileOnly("io.projectreactor.addons:reactor-extra:3.3.3.RELEASE")


}