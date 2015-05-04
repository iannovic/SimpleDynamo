package edu.buffalo.cse.cse486586.simpledynamo.Server;

import android.app.Activity;
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

import edu.buffalo.cse.cse486586.simpledynamo.data.Pojo;

/**
 * Created by ianno_000 on 5/3/2015.
 */
public class ListeningServerRunnable implements Runnable {

    private Activity activity;
    ServerSocket serverSocket;

    public static int W = 2;        //minimum number of writes that must occur
    public static int R = 2;        //minimum number of reads that must occur
    public static int N = 3;        //replication degree
    public static final int TIMEOUT_CONSTANT = 1000;
    public static final int SERVER_TIMEOUT_CONSTANT = 20000; // set this to see that the thread is moving

    public static int requestSequence;
    public static Semaphore requestLock;
    public static List<Pojo> requestList;

    public ListeningServerRunnable(Activity activity, ServerSocket serverSocket) {
        this.activity = activity;
        this.serverSocket = serverSocket;

        requestList = new LinkedList<Pojo>();
        requestLock = new Semaphore(1);
        requestSequence = 0;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Log.i("LISTENING_SERVER", "-=-~ BEGIN SERVER TASK ~-=-");
                serverSocket.setSoTimeout(SERVER_TIMEOUT_CONSTANT);

                Socket socket = serverSocket.accept();
                Log.i("LISTENING_SERVER", "ACCEPTED A NEW SOCKET CONNECTION");
                /*
                    READ input from socket after accepting it
                 */
                InputStream is = socket.getInputStream();
                ObjectInputStream ois = new ObjectInputStream(is);
                Pojo pojoReceived = (Pojo) ois.readObject();
                socket.setSoTimeout(TIMEOUT_CONSTANT);
                /*
                    ACK back to the sending process to handle failure
                 */
                Log.i("ServerTask","POJO received! " + pojoReceived.asString());
                OutputStream os = socket.getOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(os);

                if (!pojoReceived.getDestinationPort().equals(pojoReceived.getSendingPort())) {
                    oos.writeObject(pojoReceived);
                    oos.flush();
                    Log.i("ServerTask","ACK sent for pojo: " + pojoReceived.asString());
                } else {
                    Log.i("ServerTask","no need to ACK to self port " + pojoReceived.asString());

                }
                new Thread(new StateMachineRunnable(socket,activity,oos,pojoReceived,null)).start();

                //new StateMachineTask(socket,activity,oos).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,pojoReceived);
            } catch (SocketTimeoutException e) {
                Log.i("LISTENING_SERVER","-=-~ SERVER TIMED OUT ~-=-");
            }catch (IOException e) {
                Log.e("ERROR", e.getMessage(),e);
            }catch (ClassNotFoundException e) {
                Log.e("ERROR", "failed to read pojo object from stream", e);
            }

        }
    }
}
