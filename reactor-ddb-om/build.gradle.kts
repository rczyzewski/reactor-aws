plugins {
    id("io.freefair.lombok") version "4.1.2"
}

dependencies {
    compile("com.google.auto.service:auto-service:1.0-rc5")
    compile("com.squareup:javapoet:1.11.1")
    compile("org.projectlombok:lombok:1.18.10")
    compile("org.slf4j:slf4j-api:1.7.28")
    compile(project(":reactor-aws-client"))
    testCompile(project(":reactor-aws-test"))
    //annotationProcessor(project(":reactor-aws-root:reactor-ddb-om"))
    //testAnnotationProcessor(project(":reactor-aws-root:reactor-ddb-om"))
    testCompile("org.junit.jupiter:junit-jupiter-engine:5.5.2")
    testCompile("ch.qos.logback:logback-classic:1.2.3")
    testCompile("org.assertj:assertj-core:3.13.2")
    testCompile("org.mockito:mockito-junit-jupiter:3.0.0")
    testCompile("org.testcontainers:localstack:1.12.2")
    compileOnly("io.projectreactor:reactor-core:3.3.0.RELEASE")
}

tasks {
    compileTestJava {
        exclude("**/**/**")
    }
    test {
        exclude("**/**/**")
    }
}
