plugins {
    java
    `maven-publish`
    jacoco
}

allprojects {
    apply(plugin = "maven")


    group = "com.ravenpack.aws"
    version = "0.0.5-SNAPSHOT"
}

subprojects {
    apply(plugin = "java")
    apply(plugin= "maven-publish")
    apply(plugin= "jacoco")

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8

        withJavadocJar()
        withSourcesJar()
    }

    repositories {
        mavenLocal()
        mavenCentral()
    }

    tasks {

        test {
            useJUnitPlatform()

            include("**Test.*")
            include("**/**/**Test.*")
            include("**/**/**IT.*")
        }
    }

    tasks.jacocoTestReport {
        dependsOn(tasks.test) // tests are required to run before generating the report
    }

    tasks.test {
        finalizedBy(tasks.jacocoTestReport) // report is always generated after tests run
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
            maven {

                name = "RPNexus"
                url = uri("http://gitserver.research.ravenpack.com:8081/nexus/content/repositories/releases/")
                credentials {
                    username = System.getenv("RP_ACTOR")
                    password = System.getenv("RP_TOKEN")
                }
            }
        }
    }
}




