package edu.buffalo.cse.cse486586.simpledynamo.Server;

import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity;
import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider;
import edu.buffalo.cse.cse486586.simpledynamo.data.Pojo;

/**
 * Created by ianno_000 on 4/29/2015.
 */
public class StateMachineTask extends AsyncTask<Pojo,String,Void> {

    Socket requestingSocket;
    Pojo requestPojo;
    Activity activity;
    ObjectOutputStream oos;
    public StateMachineTask(Socket socket,Activity activity,ObjectOutputStream oos) {
        this.oos = oos;
        requestingSocket = socket;
        this.activity = activity;
    }
    @Override
    public Void doInBackground(Pojo... params) {
        requestPojo = params[0];

        boolean requestingProcessIsSelf = false;
        if (requestingSocket == null) {
            requestingProcessIsSelf = true;
        }

        try {
            switch (requestPojo.getType()) {
                case Pojo.TYPE_COORDINATOR_INSERT:
                    activity.getContentResolver().insert(SimpleDynamoProvider.PROVIDER_URI, requestPojo.getValues());
                    Log.i("STATE_MACHINE_INSERT", "coordinating insert.");
                /*
                            UPDATE vector clock. this counts as an event
                         */
                    Log.i("TYPE_COORDINATOR_INSERT", "updating the vector clock by 1");
                    try {
                        ProviderHelper.getInstance().semaphore.acquire();
                        ProviderHelper.getInstance().updateMyVersion();
                        ProviderHelper.getInstance().semaphore.release();

                    } catch (InterruptedException e) {
                        Log.e("ERROR", "failed during semaphore acquire in coordinater insert", e);
                    }

                        /*
                            REPLICATE on W nodes in order of preference
                         */
                    Log.i("TYPE_COORDINATOR_INSERT", "Replicating on the highest W nodes");
                    int writes = 0;
                    int i = 0;

                    while (writes < ServerTask.W && i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES - 1) {

                        String port = ProviderHelper.getInstance().preferenceList.get(i);
                        Pojo pojo = new Pojo();
                        pojo.setSendingVersion(ProviderHelper.getInstance().getVersion(SimpleDynamoActivity.MY_EMULATOR_PORT));
                        pojo.setValues(requestPojo.getValues());
                        pojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                        pojo.setType(Pojo.TYPE_NODE_INSERT);
                        pojo.setDestinationPort(port);
                            /*
                                upon successful return of this function, we know that we received an ACK from the destination process
                             */
                        Log.i("TYPE_COORDINATOR_INSERT", "sending to be replicated and written on port " + port);
                    if (ProviderHelper.getInstance().sendPojoToDestinationPort(pojo)) {
                            writes++;
                        }
                        i++;
                    }

                    Log.i("TYPE_COORDINATOR_INSERT", "populating response object for asking process");
                    Pojo coordinatorResponsePojo = new Pojo();
                    coordinatorResponsePojo.setType(Pojo.TYPE_ACK);
                    coordinatorResponsePojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                    coordinatorResponsePojo.setDestinationPort(requestPojo.getSendingPort());

                    if (writes >= ServerTask.W) {
                        coordinatorResponsePojo.setSendingVersion(ProviderHelper.getInstance().getVersion(SimpleDynamoActivity.MY_EMULATOR_PORT)); //indicates response
                        Log.i("TYPE_COORDINATOR_INSERT", "successfully written!");
                    } else {
                        coordinatorResponsePojo.setSendingVersion(0);   //indicates failure
                    }

                    Log.i("TYPE_COORDINATOR_INSERT", "responding to the asking process with pojo " + coordinatorResponsePojo.asString());
                    if (requestingProcessIsSelf) {
                        params[1].setType(coordinatorResponsePojo.getType());
                        params[1].setValues(coordinatorResponsePojo.getValues());
                        params[1].setSendingVersion(coordinatorResponsePojo.getSendingVersion());
                        params[1].setDestinationPort(coordinatorResponsePojo.getDestinationPort());
                        params[1].setSendingPort(coordinatorResponsePojo.getSendingPort());
                    } else {
                        oos.writeObject(coordinatorResponsePojo);
                    }

                    break;
                case Pojo.TYPE_COORDINATOR_QUERY:
                    Log.i("STATE_MACHINE_QUERY", "coordinating query.");
                    /*************************************************************************
                     GET key,value from local storage
                     *************************************************************************/
                    Log.i("TYPE_COORDINATOR_QUERY", "getting local key");
                    List<Pojo> readPojoList = new LinkedList<Pojo>();
                    Cursor cursor = activity.getContentResolver().query(SimpleDynamoProvider.PROVIDER_URI, null, ((String) requestPojo.getValues().get("key")), new String[]{"coordinator"}, null);
                    if (cursor != null && cursor.moveToFirst()) {
                        ContentValues values = new ContentValues();
                        String key = cursor.getString(cursor.getColumnIndex("key"));
                        String value = cursor.getString(cursor.getColumnIndex("value"));
                        values.put("key", key);
                        values.put("value", value);
                        Pojo coordinatorRetPojo = new Pojo();
                        coordinatorRetPojo.setValues(values);
                        coordinatorRetPojo.setSendingVersion(ProviderHelper.getInstance().getVersion(SimpleDynamoActivity.MY_EMULATOR_PORT));
                        readPojoList.add(coordinatorRetPojo);
                        cursor.close();
                    }


                    /***********************************************************************
                     GET R keys, values from preference list
                     ***********************************************************************/
                    Log.i("TYPE_COORDINATOR_QUERY", "asking nodes from preference list by rank.");
                    int reads = 0;
                    i = 0;
                    while (reads < ServerTask.R && i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES) {

                        String port = ProviderHelper.getInstance().preferenceList.get(i);
                        Pojo pojo = new Pojo();
                        pojo.setSendingVersion(ProviderHelper.getInstance().getVersion(SimpleDynamoActivity.MY_EMULATOR_PORT));
                        pojo.setValues(requestPojo.getValues());
                        pojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                        pojo.setType(Pojo.TYPE_NODE_QUERY);
                        pojo.setDestinationPort(port);
                        Pojo retPojo = new Pojo();          //set return values in this pojo

                        /*************************************************************************
                         upon successful return of this function, we know that we received an ACK from the destination process
                         **************************************************************************/
                        if (ProviderHelper.getInstance().sendPojoToDestinationPort(pojo, retPojo)) {
                            Log.i("TYPE_COORDINATOR_QUERY","received response from the " + i + "th ranked node");
                            reads++;
                            readPojoList.add(retPojo);
                        } else {
                            Log.e("TYPE_COORDINATOR_QUERY", "failed to query the following: " + pojo.asString());
                        }
                        i++;
                    }

                    //@TODO MUST HANDLE FAILURE IN reads < R && I >= 5
                    /***********************************************************************************
                     DETERMINE highest versioned key
                     ***********************************************************************************/
                    Log.i("TYPE_COORDINATOR_QUERY", "determining highest versioned key");
                    int maxVersion = 0;
                    int maxVersionIndex = 0;
                    for (int j = 0; j < readPojoList.size(); j++) {
                        if (readPojoList.get(j).getSendingVersion() > maxVersion) {
                            maxVersionIndex = j;
                            maxVersion = readPojoList.get(j).getSendingVersion();
                        }
                    }


                    Pojo pojoToSend = new Pojo();
                    pojoToSend.setType(Pojo.TYPE_ACK);
                    pojoToSend.setValues(readPojoList.get(maxVersionIndex).getValues());
                    pojoToSend.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                    pojoToSend.setDestinationPort(requestPojo.getSendingPort());
                    pojoToSend.setSendingVersion(maxVersion);

                    /**************************************************************************************
                     SEND response back to the requesting node
                     **************************************************************************************/
                    Log.i("TYPE_COORDINATOR_QUERY", "sending response back to requesting process");
                    if (requestingProcessIsSelf) {
                        params[1].setType(pojoToSend.getType());
                        params[1].setValues(pojoToSend.getValues());
                        params[1].setSendingVersion(maxVersion);
                        params[1].setDestinationPort(pojoToSend.getDestinationPort());
                        params[1].setSendingPort(pojoToSend.getSendingPort());
                    } else {
                        oos.writeObject(pojoToSend);
                    }


                    break;

                case Pojo.TYPE_NODE_QUERY:
                        /*
                            GET key,value
                         */
                    Log.i("TYPE_NODE_QUERY","Getting local key value pair.");
                    String key = (String) requestPojo.getValues().get("key");
                    Cursor csr = activity.getContentResolver().query(SimpleDynamoProvider.PROVIDER_URI,null,key,new String[]{"node"},null);
                    Pojo respondingPojo = new Pojo();
                    respondingPojo.setDestinationPort(requestPojo.getSendingPort());
                    respondingPojo.setType(Pojo.TYPE_ACK);
                    respondingPojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
                    ContentValues values = new ContentValues();

                    if (csr != null && csr.moveToFirst()) {
                        String value = csr.getString(csr.getColumnIndex("value"));
                        values = new ContentValues();
                        values.put("key",key);
                        values.put("value",value);
                    }
                    respondingPojo.setValues(values);
                    csr.close();

                         /*
                            SEND back to requesting process through same ACCEPTED socket.
                         */
                    Log.i("TYPE_NODE_QUERY","Sending response back to coordinator");
                    oos.writeObject(respondingPojo);

                    break;
                case Pojo.TYPE_NODE_INSERT:
                    try {
                        ProviderHelper.getInstance().semaphore.acquire();
                        ProviderHelper.getInstance().setVersion(requestPojo.getSendingPort(),requestPojo.getSendingVersion());
                        ProviderHelper.getInstance().semaphore.release();

                    } catch (InterruptedException e) {
                        Log.e("ERROR","failed during semaphore acquire in coordinater insert",e);
                    }
                    activity.getContentResolver().insert(SimpleDynamoProvider.PROVIDER_URI,requestPojo.getValues());
                    break;
            }
        }catch (IOException e) {
            Log.e("EXCEPTION",e.getMessage(),e);
        }
        return null;
    }
}
