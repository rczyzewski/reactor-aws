plugins {
    `java-library`
    idea
    `maven-publish`
    id("io.freefair.lombok") version "5.1.0"
}


dependencies {


    api(platform("software.amazon.awssdk:bom:2.13.55"))
    api(platform("org.testcontainers:testcontainers-bom:1.14.3"))
    //https://mvnrepository.com/artifact/org.junit/junit-bom/5.6.2
    api(platform("org.junit:junit-bom:5.6.2"))
    api(platform("io.projectreactor:reactor-bom:Dysprosium-SR9"))


    api("com.squareup:javapoet:1.13.0")
    api("io.projectreactor:reactor-core")

    compileOnly("org.slf4j:slf4j-api:1.7.28")

    api(project(":reactor-aws-client"))

    //implementation("com.google.auto.service:auto-service:1.0-rc6")
    annotationProcessor("com.google.auto.service:auto-service:1.0-rc6")
    api("com.google.auto.service:auto-service:1.0-rc6")

    compileOnly("org.jetbrains:annotations:19.0.0")

    testImplementation("ch.qos.logback:logback-classic:1.2.3")

    testImplementation("org.assertj:assertj-core:3.13.2")
    testImplementation("org.mockito:mockito-junit-jupiter:3.0.0")
    testImplementation("io.projectreactor:reactor-test")

    testImplementation("org.testcontainers:localstack")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.testcontainers:junit-jupiter")

    testImplementation(project(":reactor-aws-test"))

    //testCompileOnly(project(":reactor-ddb-om"))
    testAnnotationProcessor(project(":reactor-ddb-om"))
}

sourceSets["test"].java {
}


tasks.named<Test>("test") {
    useJUnitPlatform()
}



