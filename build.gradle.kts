plugins {
    java
}

allprojects {
    apply(plugin = "maven")

    group = "com.ravenpack.aws"
    version = "1.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java")

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
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
}
