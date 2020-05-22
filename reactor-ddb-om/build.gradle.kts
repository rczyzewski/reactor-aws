plugins {
    `java-library`
    idea
    `maven-publish`
    id("io.freefair.lombok") version "5.0.1"
}


dependencies {
    compile("com.squareup:javapoet:1.11.1")
    runtimeOnly("com.squareup:javapoet:1.11.1")
    testCompileOnly("com.squareup:javapoet:1.11.1")
    compile("io.projectreactor:reactor-core:3.3.0.RELEASE")


    compile("org.slf4j:slf4j-api:1.7.28")
    compile(project(":reactor-aws-client"))
    implementation("com.google.auto.service:auto-service:1.0-rc6")
    compile("com.google.auto.service:auto-service:1.0-rc6")

    //runtimeOnly("com.google.auto.service:auto-service:1.0-rc6")
    annotationProcessor("com.google.auto.service:auto-service:1.0-rc6")


    testCompile("ch.qos.logback:logback-classic:1.2.3")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.2.3")

    testCompile("org.assertj:assertj-core:3.13.2")
    testCompile("org.mockito:mockito-junit-jupiter:3.0.0")
    testCompile("io.projectreactor:reactor-test:3.3.0.RELEASE")

    testRuntimeOnly("org.testcontainers:localstack:1.14.1")
    testCompileOnly("org.testcontainers:localstack:1.14.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")


    compileOnly("software.amazon.awssdk:dynamodb:2.13.8")
    compile("software.amazon.awssdk:dynamodb:2.13.8")
    testCompileOnly("software.amazon.awssdk:dynamodb:2.13.8")
    testRuntimeOnly("software.amazon.awssdk:dynamodb:2.13.8")
    testRuntimeOnly("com.amazonaws:aws-java-sdk-dynamodb:1.11.781")


    testCompileOnly(project(":reactor-ddb-om"))
    testAnnotationProcessor(project(":reactor-ddb-om"))

}

sourceSets["test"].java {
}


tasks.named<Test>("test") {
    useJUnitPlatform()
}



