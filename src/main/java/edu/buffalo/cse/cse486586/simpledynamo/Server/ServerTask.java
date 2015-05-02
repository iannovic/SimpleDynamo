package edu.buffalo.cse.cse486586.simpledynamo.Server;

import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity;
import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider;
import edu.buffalo.cse.cse486586.simpledynamo.data.Pojo;

/**
 * Created by ianno_000 on 4/25/2015.
 */
public class ServerTask extends AsyncTask<ServerSocket,String,Void>{
    private Activity activity;

    /*
        use these variables to handle concurrency for the coordinator and provider on the same process
     */
    private Semaphore coordinatorLock;
    private List<Pojo> coordinatorPojoList;


    public static int W = 2;        //minimum number of writes that must occur
    public static int R = 2;        //minimum number of reads that must occur
    public static int N = 3;        //replication degree
    private final int TIMEOUT_CONSTANT = 100;

    public ServerTask(Activity activity) {
        this.activity = activity;
        coordinatorLock = new Semaphore(1);
        coordinatorPojoList = new LinkedList<Pojo>();
    }

    @Override
    protected Void doInBackground(ServerSocket... params) {

        ServerSocket serverSocket = params[0];

        while (true) {
            try {
                Log.i("SERVER_TASK","!!!!!!!!!!!!!!BEGIN SERVER TASK!!!!!!!!!!!!!!!!!!!!!!");
                Socket socket = serverSocket.accept();
                socket.setSoTimeout(TIMEOUT_CONSTANT);
                /*
                    READ input from socket after accepting it
                 */
                InputStream is = socket.getInputStream();
                ObjectInputStream ois = new ObjectInputStream(is);
                Pojo pojoReceived = (Pojo) ois.readObject();
                /*
                    ACK back to the sending process to handle failure
                 */
                Log.i("ServerTask","POJO received! " + pojoReceived.asString());
                OutputStream os = socket.getOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);

                Pojo responsePojo = new Pojo();
                responsePojo.setType(Pojo.TYPE_ACK);
                responsePojo.setDestinationPort(pojoReceived.getDestinationPort());
                responsePojo.setSendingPort(pojoReceived.getSendingPort());
                responsePojo.setSendingVersion(pojoReceived.getSendingVersion());
                responsePojo.setValues(pojoReceived.getValues());


                if (socket.isClosed()) {
                    Log.e("ServerTask","SOCKET IS CLOSED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                } else {
                    Log.e("ServerTask","socket is not closed. yay.");
                }
                if (!pojoReceived.getDestinationPort().equals(pojoReceived.getSendingPort())) {
                    oos.writeObject(responsePojo);
                    oos.flush();
                    Log.i("ServerTask","ACK sent for pojo: " + pojoReceived.asString());
                } else {
                    Log.i("ServerTask","no need to ACK to self port " + pojoReceived.asString());

                }
                new StateMachineTask(socket,activity,oos).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,pojoReceived);
            } catch (SocketTimeoutException e) {
                Log.e("ERROR","SOCKET TIMED OUT",e);
            }catch (IOException e) {
                Log.e("ERROR", e.getMessage(),e);
            }catch (ClassNotFoundException e) {
                Log.e("ERROR", "failed to read pojo object from stream", e);
            }

        }
    }
}
