package dev.rivet.smoke;

import android.app.Activity;
import android.content.Context;
import android.net.ConnectivityManager;
import android.net.Network;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.widget.ScrollView;
import android.widget.TextView;
import java.io.File;
import java.util.ArrayList;

/** Dedicated ordinary-app verification; no VPN service, hidden API, or root. */
public final class MainActivity extends Activity {
    private static final Handler MAIN = new Handler(Looper.getMainLooper());
    // Only the main thread accesses these fields. Views are detached in onStop.
    private static final ArrayList<TextView> activeViews = new ArrayList<>();
    private static boolean started;
    private static String result;
    private TextView text;
    private static native String runNative(long activeNetwork, int apiLevel);

    @Override public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        text = new TextView(this);
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
        if (!started) {
            started = true;
            Context application = getApplicationContext();
            new Thread(() -> runSmoke(application), "RivetSmoke").start();
        }
    }

    @Override protected void onStart() {
        super.onStart();
        activeViews.add(text);
        if (result != null) { text.setText(result); }
    }

    @Override protected void onStop() {
        activeViews.remove(text);
        super.onStop();
    }

    private static void runSmoke(Context application) {
        File destination = new File(application.getFilesDir(), "smoke-result.json");
        String json = SmokeResult.run(destination, () -> {
            System.loadLibrary("rivet_android_smoke");
            ConnectivityManager connectivity = (ConnectivityManager)
                application.getSystemService(Context.CONNECTIVITY_SERVICE);
            Network network = connectivity.getActiveNetwork();
            return runNative(network == null ? 0 : network.getNetworkHandle(), Build.VERSION.SDK_INT);
        });
        Log.i("RivetSmoke", json);
        MAIN.post(() -> {
            result = json;
            for (TextView view : activeViews) { view.setText(json); }
        });
    }
}
