package edu.buffalo.cse.cse486586.simpledynamo.test.testButtons;

import android.view.View;

import edu.buffalo.cse.cse486586.simpledynamo.test.ConnectionTestRunnable;

/**
 * Created by ianno_000 on 5/4/2015.
 */
public class ConnectionTestClickListener implements View.OnClickListener {

    @Override
    public void onClick(View v) {
        new Thread(new ConnectionTestRunnable()).start();
    }
}
