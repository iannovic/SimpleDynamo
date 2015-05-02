package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.graphics.Matrix;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import edu.buffalo.cse.cse486586.simpledynamo.Server.ProviderHelper;
import edu.buffalo.cse.cse486586.simpledynamo.Server.StateMachineTask;
import edu.buffalo.cse.cse486586.simpledynamo.data.DatabaseHelper;
import edu.buffalo.cse.cse486586.simpledynamo.data.DynamoDAO;
import edu.buffalo.cse.cse486586.simpledynamo.data.Pojo;

public class SimpleDynamoProvider extends ContentProvider {

    public static final String CONTENT_URI = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
    public static final Uri PROVIDER_URI = Uri.parse(CONTENT_URI);

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

        Log.i("INSERT", values.toString());
        Context context = getContext();
        DynamoDAO dao = new DynamoDAO(context);
        String value;

        //This case implies that the value is ready to be inserted into this node. otherwise it is a new query and needs to be handled properly.

        try {
            if (((value = (String) values.get("coordinator")) != null &&
                    !value.equals(""))) {

                Log.i(SimpleDynamoActivity.TAG, "*******************INSERTING THE VALUE*******************");
                values.remove("coordinator");
                dao.open();
                dao.insertMessage(values);

            } else if (((value = (String) values.get("node")) != null &&
                    !value.equals(""))) {

                Log.i(SimpleDynamoActivity.TAG, "*******************INSERTING THE VALUE*******************");
                values.remove("node");
                dao.open();
                dao.insertMessage(values);

            } else {
                Log.i("INSERT","requesting coordinator for key value");
                String key = (String) values.get("key");
                String keyId;
                String successorPort;
                keyId = genHash(key);
                successorPort = ProviderHelper.getInstance().findSuccessor(keyId);

                Pojo pojo = new Pojo();
                pojo.setValues(values);
                pojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                pojo.setType(Pojo.TYPE_COORDINATOR_INSERT);
                pojo.setDestinationPort(successorPort);
                Pojo responsePojo = new Pojo();
                responsePojo.setSendingVersion(0);
                responsePojo.setType(100);
                Log.i("QUERY","coordinator is " + successorPort);

                /*
                    added this to handle the case that the requesting process is the coordinator, if this is the case then we have to make a new state machine task locally instead of through a connection
                 */

                if (successorPort.equals(SimpleDynamoActivity.MY_EMULATOR_PORT)) {
                    Log.i("INSERT","coordinator is self. creating new StateMachineTask");
                    new StateMachineTask(null,SimpleDynamoActivity.activity,null).doInBackground(pojo,responsePojo);
                    Log.i("INSERT","WE HAVE A RESULT FROM THE LOCAL COORDINATOR! " + responsePojo.asString());
                } else {
                    if (!ProviderHelper.getInstance().sendPojoToDestinationPort(pojo,responsePojo)) {
                        Log.e("ERROR", "FAILED TO GET ACK");
                        return null;
                    } else if (responsePojo.getSendingVersion() != 0){
                        Log.i("QUERY","REQUEST SUCCESSFUL ... received ACK from coordinator!");
                    } else {
                        Log.e("ERROR","Coordinator did not replicate properly. insert was a failure");
                        return null;
                    }
                }

            }
        } catch (NoSuchAlgorithmException e) {
            Log.e("ERROR", "while trying to insert new string", e);
        } catch (SQLException e) {
            Log.e("ERROR", "SQL error", e);
        }
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        Cursor cursor = null;
        try {

            if (selection != null && selection.equals(DatabaseHelper.GLOBAL_KEY)) {
                Log.i(SimpleDynamoActivity.TAG, "*******************GLOBAL QUERY*******************");
            } else if (selection != null && selection.equals(DatabaseHelper.LOCAL_KEY)) {
                Log.i(SimpleDynamoActivity.TAG, "*******************LOCAL QUERY*******************");
                Context context = getContext();
                DynamoDAO dao = new DynamoDAO(context);
                dao.open();
                cursor = dao.queryMessage(selection);
            } else if (selection != null) {
                Log.i(SimpleDynamoActivity.TAG, "*******************SINGLE QUERY*******************");
                if (selectionArgs == null) {
                    Log.i(SimpleDynamoActivity.TAG, "requesting process for query");
                    Pojo pojo = new Pojo();
                    String keyId = genHash(selection);
                    ContentValues values = new ContentValues();
                    values.put("key",selection);
                    pojo.setDestinationPort(ProviderHelper.getInstance().findSuccessor(keyId));
                    pojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                    pojo.setType(Pojo.TYPE_COORDINATOR_QUERY);
                    pojo.setSendingVersion(0);
                    pojo.setValues(values);
                    Pojo retPojo = new Pojo();
                    retPojo.setType(100);

                    Log.i(SimpleDynamoActivity.TAG, "sending query request to the coordinator with object: " + pojo.asString());
                    if (pojo.getDestinationPort().equals(pojo.getSendingPort())) {
                        Log.i("QUERY_REQUEST","coordinator is self. creating StateMachineTask");
                        new StateMachineTask(null,SimpleDynamoActivity.activity,null).doInBackground(pojo,retPojo);
                        Log.i("QUERY_REQUEST","value has been returned! " + retPojo.asString());
                    }
                    else{
                        if (!ProviderHelper.getInstance().sendPojoToDestinationPort(pojo,retPojo)) {
                        Log.e("QUERY_REQUEST","failed");
                         } else {
                        Log.i("QUERY_REQUEST","SUCCESS! " + retPojo.asString());
                        }
                    }
                    Log.i("QUERY_REQUEST","creating cursor to return");
                    MatrixCursor mc =  new MatrixCursor(DatabaseHelper.TABLE_PROJECTION,10);
                    mc.addRow(new Object[]{retPojo.getValues().get("key"),retPojo.getValues().get("value")});
                    cursor = mc;

                } else if (selectionArgs[0] != null && (selectionArgs[0].equals("coordinator"))) {
                    Log.i(SimpleDynamoActivity.TAG, "coordinator for query");
                    Context context = getContext();
                    DynamoDAO dao = new DynamoDAO(context);
                    dao.open();
                    cursor = dao.queryMessage(selection);

                } else if (selectionArgs[0] != null && selectionArgs[0].equals("node")) {
                    Log.i(SimpleDynamoActivity.TAG, "node for query");
                    Context context = getContext();
                    DynamoDAO dao = new DynamoDAO(context);
                    dao.open();
                    cursor = dao.queryMessage(selection);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e("ERROR", "while trying to insert new string", e);
        } catch (SQLException e) {
            Log.e("ERROR","SQL error",e);
        } /*catch (InterruptedException e) {
            Log.e("ERROR","",e);
        }*/
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
