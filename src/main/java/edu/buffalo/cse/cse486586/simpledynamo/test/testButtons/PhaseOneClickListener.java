package edu.buffalo.cse.cse486586.simpledynamo.test.testButtons;

import android.app.Activity;
import android.content.ContentValues;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;

import edu.buffalo.cse.cse486586.simpledynamo.test.PhaseOneTestTask;

/**
 * Created by ianno_000 on 4/29/2015.
 */
public class PhaseOneClickListener implements View.OnClickListener {

    private Activity activity;
    public PhaseOneClickListener(Activity activity) {
        this.activity = activity;
    }
    @Override
    public void onClick(View v) {

        new PhaseOneTestTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,activity);


    }
}
