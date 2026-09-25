package dev.rivet.smoke;

import android.app.Activity;
import android.content.Context;
import android.net.ConnectivityManager;
import android.net.Network;
import android.os.Bundle;
import android.util.Log;
import android.widget.ScrollView;
import android.widget.TextView;
import java.io.File;
import java.util.concurrent.CompletableFuture;

/** Dedicated ordinary-app verification; no VPN service, hidden API, or root. */
public final class MainActivity extends Activity {
    private static CompletableFuture<String> result;
    private static native String runNative(long activeNetwork);

    @Override public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        TextView text = new TextView(this);
        text.setTextIsSelectable(true);
        text.setTextSize(13);
        text.setTypeface(android.graphics.Typeface.MONOSPACE);
        text.setContentDescription("Rivet structured native smoke results");
        int padding = (int) (16 * getResources().getDisplayMetrics().density);
        text.setPadding(padding, padding, padding, padding);
        text.setText(SmokeResult.RUNNING);
        ScrollView scroll = new ScrollView(this);
        scroll.addView(text);
        scroll.setOnApplyWindowInsetsListener((view, insets) -> {
            view.setPadding(insets.getSystemWindowInsetLeft(), insets.getSystemWindowInsetTop(),
                insets.getSystemWindowInsetRight(), insets.getSystemWindowInsetBottom());
            return insets;
        });
        setContentView(scroll);
        scroll.requestApplyInsets();

        // No incoming Intent, URI, nested Intent, or extras influence native
        // execution. The exported launcher can only run fixed loopback checks.
        synchronized (MainActivity.class) {
            if (result == null) {
                Context application = getApplicationContext();
                result = CompletableFuture.supplyAsync(() -> {
                    File destination = new File(application.getFilesDir(), "smoke-result.json");
                    String json = SmokeResult.run(destination, () -> {
                        System.loadLibrary("rivet_android_smoke");
                        ConnectivityManager connectivity = (ConnectivityManager)
                            application.getSystemService(Context.CONNECTIVITY_SERVICE);
                        Network network = connectivity.getActiveNetwork();
                        return runNative(network == null ? 0 : network.getNetworkHandle());
                    });
                    Log.i("RivetSmoke", json);
                    return json;
                });
            }
            result.thenAccept(json -> runOnUiThread(() -> text.setText(json)));
        }
    }
}
