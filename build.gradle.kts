plugins {
    java
    application
    id("com.google.cloud.tools.jib") version "3.3.1"
}

//apply(plugin = "java")

group = "org.simmi"
version = "1.0"

jib {
    from {
        image = "public.ecr.aws/l8m2k1n1/netapp/spark:graalvm-22.3.1"
        if (project.hasProperty("REGISTRY_USER")) {
            auth {
                username = project.findProperty("REGISTRY_USER")?.toString()
                password = project.findProperty("REGISTRY_PASSWORD")?.toString()
            }
        }
    }
    to {
        image = project.findProperty("APPLICATION_REPOSITORY")?.toString() ?: "public.ecr.aws/l8m2k1n1/netapp/spark:flightconnect-graalvm-22.3.1"
        //tags = [project.findProperty("APPLICATION_TAG")?.toString() ?: "1.0"]
        if (project.hasProperty("REGISTRY_USER")) {
            var reg_user = project.findProperty("REGISTRY_USER")?.toString()
            var reg_pass = project.findProperty("REGISTRY_PASSWORD")?.toString()
            System.err.println("hello2 " + reg_user + " " + reg_pass)
            auth {
                username = reg_user
                password = reg_pass
            }
        }
    }
    containerizingMode = "packaged"
    container {
        user = "app"
        entrypoint = listOf("/opt/entrypoint.sh")
        workingDirectory = "/opt/spark/work-dir/"
        appRoot = "/opt/spark/"
        //mainClass = "org.simmi.SparkFlightConnect"
        environment = mapOf("JAVA_TOOL_OPTIONS" to "-Djdk.lang.processReaperUseDefaultStackSize=true --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED",
                "SPARK_EXTRA_CLASSPATH" to "/opt/spark/classes/*")
    }
}

application {
    mainClass.set("org.simmi.SparkFlightConnect")
    applicationDefaultJvmArgs = listOf("--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson:jackson-bom:2.14.1")
    implementation("org.apache.arrow:flight-core:11.0.0")
    implementation("org.apache.arrow:arrow-flight:11.0.0")
    implementation("org.apache.spark:spark-core_2.12:3.3.1")
    implementation("org.apache.spark:spark-sql_2.12:3.3.1")
    implementation("org.apache.spark:spark-kubernetes_2.12:3.3.1")
    implementation("org.apache.spark:spark-hadoop-cloud_2.12:3.3.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}