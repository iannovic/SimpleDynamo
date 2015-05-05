package edu.buffalo.cse.cse486586.simpledynamo.test;

import android.util.Log;

import java.util.concurrent.Semaphore;

import edu.buffalo.cse.cse486586.simpledynamo.Server.ProviderHelper;
import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity;
import edu.buffalo.cse.cse486586.simpledynamo.data.Pojo;

/**
 * Created by ianno_000 on 5/4/2015.
 */
public class ConnectionTestRunnable implements Runnable {

    static Semaphore semaphore = new Semaphore(1);
    static int counter = 0;
    @Override
    public void run() {

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        counter++;
        semaphore.release();

        for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES; i++) {
            Pojo pojo = new Pojo();
            pojo.setType(Pojo.TYPE_CONNECTION_TEST);
            pojo.setRequestSequence(counter);
            pojo.setSendingVersion(counter);
            pojo.setSendingPort(SimpleDynamoActivity.MY_EMULATOR_PORT);
            pojo.setDestinationPort(SimpleDynamoActivity.EMULATOR_PORTS_ARRAY[i]);

            if (!pojo.getDestinationPort().equals(pojo.getSendingPort())) {
                if (ProviderHelper.getInstance().sendPojoToDestinationPort(pojo)) {
                    Log.i("CONNECTION_TEST", "SUCCESS!");
                } else {
                    Log.i("CONNECTION_TEST", "FAILURE!");
                }
            }
        }


    }
}
