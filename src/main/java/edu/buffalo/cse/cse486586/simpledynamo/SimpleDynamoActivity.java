package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.IOException;
import java.net.ServerSocket;

import edu.buffalo.cse.cse486586.simpledynamo.Server.ServerTask;
import edu.buffalo.cse.cse486586.simpledynamo.test.testButtons.PhaseOneClickListener;

public class SimpleDynamoActivity extends Activity {

    public static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    public static final String REMOTE_PORT0 = "11108";
    public static final String REMOTE_PORT1 = "11112";
    public static final String REMOTE_PORT2 = "11116";
    public static final String REMOTE_PORT3 = "11120";
    public static final String REMOTE_PORT4 = "11124";
    public static final String EMULATOR_PORT0 = "5554";
    public static final String EMULATOR_PORT1 = "5556";
    public static final String EMULATOR_PORT2 = "5558";
    public static final String EMULATOR_PORT3 = "5560";
    public static final String EMULATOR_PORT4 = "5562";
    public static final String[] EMULATOR_PORTS_ARRAY = {EMULATOR_PORT0,EMULATOR_PORT1,EMULATOR_PORT2,EMULATOR_PORT3,EMULATOR_PORT4};
    public static final String[] REMOTE_PORTS_ARRAY = {REMOTE_PORT0,REMOTE_PORT1,REMOTE_PORT2,REMOTE_PORT3,REMOTE_PORT4};
    public static final int SERVER_PORT = 10000;

    public static int MAX_NUMBER_OF_PROCESSES = 5;
    public static String MY_REMOTE_PORT;
    public static String MY_EMULATOR_PORT;
    public static Activity activity;
    @Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
        activity = this;
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /************************************
         SET UP SERVER SOCKET
         ************************************/
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        MY_EMULATOR_PORT = String.valueOf((Integer.parseInt(portStr)));
        MY_REMOTE_PORT = myPort;

        Log.i(this.TAG,"emulator port is :" + MY_EMULATOR_PORT + " remote port is : " + MY_REMOTE_PORT);

        findViewById(R.id.button1).setOnClickListener(new PhaseOneClickListener(this));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask(this).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            Log.e(TAG,e.getMessage());
            return;
        }
        /************************************
         END OF SERVER SOCKET SETUP
         ************************************/
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
