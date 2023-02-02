plugins {
    java
    application
}

group = "org.simmi"
version = "1.0-SNAPSHOT"

//apply(plugin = "java")

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
    implementation("org.apache.arrow:flight-core:10.0.1")
    implementation("org.apache.arrow:arrow-flight:10.0.1")
    implementation("org.apache.spark:spark-core_2.12:3.3.1")
    implementation("org.apache.spark:spark-sql_2.12:3.3.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.1")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}