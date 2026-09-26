package dev.rivet.smoke;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

/** File-based reports must describe this run, including failures before JNI loads. */
final class SmokeResult {
    static final String RUNNING = "{\"status\":\"running\",\"context\":\"ordinary Android App\"}";

    static String run(File destination, Callable<String> suite) {
        String json;
        try {
            invalidate(destination);
            write(destination, RUNNING);
            json = suite.call();
            if (json == null) { throw new IOException("JNI returned no smoke result"); }
        } catch (Throwable failure) {
            json = failed("app-or-jni", failure);
        }
        try {
            write(destination, json);
        } catch (Throwable failure) {
            // An unwritable report is a failed run, never a displayed pass. If
            // publication partly wrote a passing result, remove that result.
            try { invalidate(destination); }
            catch (Throwable invalidation) { failure.addSuppressed(invalidation); }
            json = failed("result-write", failure);
        }
        return json;
    }

    private static void invalidate(File destination) throws IOException {
        if (!destination.delete() && destination.exists()) {
            throw new IOException("Could not remove previous smoke result: " + destination);
        }
    }

    private static void write(File destination, String json) throws IOException {
        try (FileOutputStream output = new FileOutputStream(destination)) {
            output.write(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String failed(String stage, Throwable failure) {
        failure.printStackTrace();
        return "{\"status\":\"failed\",\"stage\":\"" + stage + "\",\"error\":"
            + quote(failure.toString()) + "}";
    }

    private static String quote(String value) {
        StringBuilder json = new StringBuilder("\"");
        final String hex = "0123456789abcdef";
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '"' || character == '\\') { json.append('\\').append(character); }
            else if (character < 0x20) {
                json.append("\\u00").append(hex.charAt(character >> 4)).append(hex.charAt(character & 15));
            } else { json.append(character); }
        }
        return json.append('"').toString();
    }
}
