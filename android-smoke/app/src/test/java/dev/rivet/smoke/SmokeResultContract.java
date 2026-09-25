package dev.rivet.smoke;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Standalone JVM regression for the same writer used by the Android activity. */
public final class SmokeResultContract {
    public static void main(String[] arguments) throws Exception {
        Path directory = Files.createTempDirectory("rivet-smoke-result-");
        Path destination = directory.resolve("smoke-result.json");
        try {
            Files.write(destination, "{\"status\":\"passed\",\"run\":\"previous\"}".getBytes(StandardCharsets.UTF_8));
            String report = SmokeResult.run(destination.toFile(), () -> {
                require(read(destination).equals(SmokeResult.RUNNING), "previous passing result survived until JNI setup");
                // Exercise an actual native loader failure without loading or
                // mocking Android networking or claiming any VPN protection.
                System.load(directory.resolve(System.mapLibraryName("missing_rivet_smoke")).toString());
                throw new AssertionError("loading an absent library succeeded");
            });
            require(report.contains("\"status\":\"failed\"") && report.contains("\"stage\":\"app-or-jni\""),
                "native setup failure was not reported as a failed current run");
            require(report.contains("UnsatisfiedLinkError"), "native load path did not reach the intended failure");
            require(read(destination).equals(report), "on-screen JNI failure was not persisted in smoke-result.json");
            System.out.println("PASS: previous pass invalidated before JNI; actual native loader failure persisted");
        } finally {
            Files.deleteIfExists(destination);
            Files.deleteIfExists(directory);
        }
    }

    private static String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static void require(boolean condition, String message) {
        if (!condition) { throw new AssertionError(message); }
    }
}
