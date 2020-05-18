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



    testCompile("org.junit.jupiter:junit-jupiter-api:5.5.2")
    testCompile(project(":reactor-aws-test"))
    testCompile("org.junit.jupiter:junit-jupiter-engine:5.5.2")
    testCompile("org.mockito:mockito-junit-jupiter:3.0.0")
    testCompile("io.projectreactor:reactor-test:3.3.5.RELEASE")
    testCompile("org.assertj:assertj-core:3.13.2")


    testCompileOnly("software.amazon.kinesis:amazon-kinesis-client:2.2.3")
    testCompileOnly("software.amazon.awssdk:dynamodb:2.13.8")
    testCompileOnly("software.amazon.awssdk:sqs:2.13.8")
    testCompileOnly("software.amazon.awssdk:s3:2.13.8")

    testRuntimeOnly("software.amazon.kinesis:amazon-kinesis-client:2.2.3")
    testRuntimeOnly("software.amazon.awssdk:dynamodb:2.13.8")
    testRuntimeOnly("software.amazon.awssdk:sqs:2.13.8")
    testRuntimeOnly("software.amazon.awssdk:s3:2.13.8")

    compileOnly("io.projectreactor:reactor-core:3.3.5.RELEASE")
    compileOnly("io.projectreactor.addons:reactor-extra:3.3.3.RELEASE")


}


publishing {
    publications {
        create<MavenPublication>("default") {
            from(components["java"])
        }
    }

    repositories {
        mavenLocal()
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/rczyzewski/reactor-aws")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}