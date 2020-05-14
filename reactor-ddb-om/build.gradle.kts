plugins {
    java
    idea
    id("io.freefair.lombok") version "4.1.2"
    id("name.remal.apt") version "1.0.190"
}


dependencies {
    compile("com.squareup:javapoet:1.11.1")
    compile("io.projectreactor:reactor-core:3.3.0.RELEASE")

    
    compile("org.slf4j:slf4j-api:1.7.28")
    compile(project(":reactor-aws-client"))
    compile("com.google.auto.service:auto-service:1.0-rc6")
    annotationProcessor("com.google.auto.service:auto-service:1.0-rc6")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")

    testCompile("ch.qos.logback:logback-classic:1.2.3")
    testCompile("org.assertj:assertj-core:3.13.2")
    testCompile("org.mockito:mockito-junit-jupiter:3.0.0")
    testCompile("org.testcontainers:localstack:1.14.1")
    testCompile("io.projectreactor:reactor-test:3.3.0.RELEASE")

    testRuntime("org.testcontainers:localstack:1.14.1")
    testCompile("org.testcontainers:localstack:1.14.1")

    compile("software.amazon.awssdk:dynamodb:2.9.16")
    compile("com.amazonaws:aws-java-sdk-dynamodb:1.11.781")
    testAnnotationProcessor(project(":reactor-ddb-om"))
}

sourceSets["test"].java {
}


tasks.named<Test>("test") {
    useJUnitPlatform()

}

