package edu.buffalo.cse.cse486586.simpledynamo.Server;

import android.app.Activity;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Reader;
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
    public static final int TIMEOUT_CONSTANT = 500;

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
                Socket socket = serverSocket.accept();
                Log.i("LISTENING_SERVER", "ACCEPTED A NEW SOCKET CONNECTION");
                new Thread(new StateMachineRunnable(socket, activity, null, null)).start();

            } catch (SocketTimeoutException e) {
                Log.i("LISTENING_SERVER","-=-~ SERVER TIMED OUT ~-=-");
            }catch (IOException e) {
                Log.e("ERROR", e.getMessage(),e);
            }
        }
    }
}
