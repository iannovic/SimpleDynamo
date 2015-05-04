package edu.buffalo.cse.cse486586.simpledynamo.test;

import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider;

/**
 * Created by ianno_000 on 4/29/2015.
 */
public class PhaseOneTestTask extends AsyncTask<Activity,String,Void>{
    @Override
    protected Void doInBackground(Activity... params) {
        Log.i("TEST", "beginning phase one test");
        Log.i("TEST","inserting first value");

        ContentValues values = new ContentValues();
        values.put("key","key1");
        values.put("value","value1");
        if (null != params[0].getContentResolver().insert(SimpleDynamoProvider.PROVIDER_URI, values)) {
            Cursor csr = params[0].getContentResolver().query(SimpleDynamoProvider.PROVIDER_URI,null,"key1",null,null);

            Log.i("TEST","ATTEMPTING TO READ CURSOR");
            if (csr != null && csr.moveToFirst()) {
                Log.i("QUERY SUCCESS KV IS ", csr.getString(csr.getColumnIndex("key")) + ", " + csr.getString(csr.getColumnIndex("value")));
            }
            csr.close();
        } else {
            Log.i("TEST","failed to insert");
        }


        return null;
    }
}
