plugins {
    java
    `maven-publish`

}

allprojects {
    apply(plugin = "maven")


    group = "com.ravenpack.aws"
    version = "0.0.1-alpha"
}

subprojects {
    apply(plugin = "java")
    apply(plugin= "maven-publish")

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
            include("**/**/**Test.*")
        }
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




