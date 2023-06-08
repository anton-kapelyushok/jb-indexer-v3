plugins {
    `java-library`
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")
    implementation("io.methvin:directory-watcher:0.18.0")
    implementation("com.google.guava:guava:32.0.0-jre")
    implementation("it.unimi.dsi:fastutil:8.5.12")
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testImplementation("com.willowtreeapps.assertk:assertk:0.26.1")
    testImplementation("io.mockk:mockk:1.13.5")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.1")
    testImplementation("commons-io:commons-io:2.12.0")
}