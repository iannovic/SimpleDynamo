package edu.buffalo.cse.cse486586.simpledynamo.Server;

import android.util.Log;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity;
import edu.buffalo.cse.cse486586.simpledynamo.data.Pojo;

/**
 * Created by ianno_000 on 4/25/2015.
 */
public class ProviderHelper {

    /*
      SINGLETON pattern for this class
     */
    private static ProviderHelper instance = null;
    public static ProviderHelper getInstance() {
        if (instance == null) {
            instance = new ProviderHelper();
        }
        return instance;
    }


    private class Node {

        String port;
        boolean isInNetwork;
        String successor;
        String predecessor;


        public Node(String port) {
            this.port = port;
            successor = "";
            predecessor = "";
        }

    }
    private ArrayList<Node> list;
    List<String> preferenceList;  //preference list mentioned in the dynamo paper.
    private Map<String,Integer> vectorClock;
    public Semaphore semaphore;

    public ProviderHelper() {
        list = new ArrayList<Node>();
        for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES; i++) {
            String id = SimpleDynamoActivity.EMULATOR_PORTS_ARRAY[i];
            Node node = new Node(id);
            list.add(node);
        }
        semaphore = new Semaphore(1);
        initializeLinks();
        initializePreferenceList();
        initializeVectorClock();
        printPreferenceList();
    }

    public void printPreferenceList() {
        Log.i("PREF LIST PRINTING","===========================================" + SimpleDynamoActivity.MY_EMULATOR_PORT);
        for (int i = 0; i < preferenceList.size(); i ++) {
            Log.i("PREF LIST PRINT",preferenceList.get(i));
        }
        Log.i("PREF LIST PRINTING","===========================================" + SimpleDynamoActivity.MY_EMULATOR_PORT);
    }
    private void initializeVectorClock() {
        vectorClock = new HashMap<String,Integer>();
        for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES; i ++) {
            vectorClock.put(SimpleDynamoActivity.EMULATOR_PORTS_ARRAY[i],0);
        }
    }

    private void initializePreferenceList() {
        preferenceList = new LinkedList<String>();

        Node node = getNode(SimpleDynamoActivity.MY_EMULATOR_PORT);
        for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES - 1; i ++) {
            preferenceList.add(node.successor);
            node = getNode(node.successor);
        }
        Log.i("CREATED PREFERENCE LIST",preferenceList.toString());
    }
    private void initializeLinks() {
        Log.i("NODE_CHANGES", "getNodeChanges() invoked");
        for (int i = 0; i < list.size(); i ++) {
            Node node = list.get(i);
            determineNodeLinks(node.port);
        }
        printLinks();
    }

    private Node determineNodeLinks(String port) {
        //Log.i("DETERMINE_LINKS", port + ":BEGINNING");
        Node retNode = new Node(port);
        String final_successor = "";
        String final_predecessor = "";

        try {
            String myNodeId = genHash(port);
            String currentSuccessor = myNodeId;
            String currentPredecessor = myNodeId;
            int nearestBehind = 0;
            int furthestAhead = 0;
            int furthestAheadIndex = 0;

            int nearestAhead = 0;
            int furthestBehind = 0;
            int furthestBehindIndex = 0;

            for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES; i++) {
                String tempPort = list.get(i).port;
                String tempNode = genHash(tempPort);

                //Log.i(TAG,tempNode + ":" + myNodeId + ":" + currentPredecessor + ":" + currentSuccessor);
                int distance = tempNode.compareTo(myNodeId);
               // Log.i("DETERMINE_LINKS", tempPort + ":" + distance);
                if (nearestAhead > 0 && distance > 0 && distance < nearestAhead) {
                    nearestAhead = distance;
                  //  Log.i("DETERMINE_LINKS", tempPort + ":" + distance + ": SET SUCCESSOR");
                    final_successor = tempPort;

                } else if (nearestAhead == 0 && distance > 0) {
                    nearestAhead = distance;
                    final_successor = tempPort;
                }

                if (furthestBehind > distance) {
                    furthestBehind = distance;
                    furthestBehindIndex = i;
                }

                if (nearestBehind < 0 && distance < 0 && distance > nearestBehind) {
                    nearestBehind = distance;
                    final_predecessor = tempPort;
                  //  Log.i("DETERMINE_LINK", tempPort + ":" + distance + ": SET PREDECESSOR");
                } else if (nearestBehind == 0 && distance < 0) {
                    nearestBehind = distance;
                    final_predecessor = tempPort;
                }

                if (furthestAhead < distance) {
                    furthestAhead = distance;
                    furthestAheadIndex = i;
                }
            }
            if (final_successor.equals("")) {
                final_successor = list.get(furthestBehindIndex).port;
              //  Log.i("DETERMINE_LINK", final_successor + ":SUCCESSOR");
            }
            if (final_predecessor.equals("")) {
                final_predecessor = list.get(furthestAheadIndex).port;
               // Log.i("DETERMINE_LINK", final_predecessor + ":PREDECESSOR");
            }

            /*
              CORNER CASE TO HANDLE WHEN THE NODE NETWORK IS OF SIZE 2
             */
            if (final_successor.equals("")) {
                final_successor = final_predecessor;
            }
            if (final_predecessor.equals("")) {
                final_predecessor = final_successor;
            }
        }catch (NoSuchAlgorithmException e) {
            Log.e("DETERMINE_LINK", "Failed to call genHash while initializing node", e);
        } finally {
            Log.i("DETERMINE_LINK", "emulator port is: " + port + " and predecessor port is: " + final_predecessor + " and successor port is: " + final_successor);
        }

        for (int i = 0; i < list.size(); i++) {
            if (port.equals(list.get(i).port)) {
                list.get(i).predecessor = final_predecessor;
                list.get(i).successor = final_successor;
            }
        }
        retNode.predecessor = final_predecessor;
        retNode.successor = final_successor;
        return retNode;
    }

    public String findSuccessor(String keyId) {
        String port = "";
        Node node = findPredecessor(keyId);
        port = node.successor;
        return port;
    }
    /**
     *
     * @param keyId ASSUMED to already be hashed
     * @return node containing the info about the predecessor
     */
    private Node findPredecessor(String keyId) {
        Node node = null;
        try {
            for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES; i++) {
                String nodeId = genHash(list.get(i).port);
                String successorId = genHash(list.get(i).successor);

                if ((keyId.compareTo(nodeId) > 0 && keyId.compareTo(successorId) < 0 && keyId.compareTo(successorId) < 0)
                        || (keyId.compareTo(nodeId) > 0 && keyId.compareTo(successorId) > 0 && nodeId.compareTo(successorId) > 0)
                        || (keyId.compareTo(nodeId) < 0 && keyId.compareTo(successorId) < 0 && nodeId.compareTo(successorId) > 0)) {
                        node = list.get(i);
                    Log.i("FIND_PREDECESSOR", "successfully found predecessor");
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e("ERROR","trying to find predecessor for key", e);
        }
        if (node == null) {
            Log.i("FIND_PREDECESSOR", "failed to find predecssor");
        }
        return node;
    }
    private Node getNode(String port) {
        Node node = null;
        for (int i = 0; i < SimpleDynamoActivity.MAX_NUMBER_OF_PROCESSES; i ++) {
            if (list.get(i).port.equals(port)) {
                node = list.get(i);
            }
        }
        return node;
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

    public void printLinks() {
        Log.i("PRINTLINK","====================================================");
        for (int i = 0; i < list.size(); i ++) {
            Node n = list.get(i);
            Log.i("PRINTLINK","Port:" + n.port + " successor:" + n.successor + " predecessor:" + n.predecessor + " inNetwork:" + n.isInNetwork );
        }
        Log.i("PRINTLINK","====================================================");
    }

    public void updateMyVersion() {
        vectorClock.put(SimpleDynamoActivity.MY_EMULATOR_PORT,vectorClock.get(SimpleDynamoActivity.MY_EMULATOR_PORT) + 1);
    }

    public void setVersion(String port, int version) {
        if (vectorClock.get(port) < version) {
            vectorClock.put(port, version);
        }
    }

    public int getVersion(String port) {
        return vectorClock.get(port);
    }

    public Boolean sendPojoToDestinationPort(Pojo pojo) {
        Socket socket = null;
        try {
            int remotePort;
            remotePort = 2 * Integer.parseInt(pojo.getDestinationPort());
            byte[] self = {10,0,2,2};

            socket = new Socket();
            InetSocketAddress sockaddr = new InetSocketAddress(InetAddress.getByAddress(self),remotePort);

            Log.i("SENDING","attempting to connect to remote port " + sockaddr.toString());
            socket.connect(sockaddr,ListeningServerRunnable.TIMEOUT_CONSTANT);
            Log.i("SENDING","successfully connected to remote port " + sockaddr.toString());
            //socket = new Socket(InetAddress.getByAddress(self),remotePort);
            socket.setSoTimeout(ListeningServerRunnable.TIMEOUT_CONSTANT);

            OutputStream stream = socket.getOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(stream);
            oos.writeObject(pojo);
            oos.flush();
            Log.i(SimpleDynamoActivity.TAG, "pojo sent: " + pojo.asString());
            Log.i(SimpleDynamoActivity.TAG, "sent pojo to destination " + pojo.getDestinationPort());

            InputStream is = socket.getInputStream();
            ObjectInputStream ois = new ObjectInputStream(is);
            Pojo pojoAck = (Pojo) ois.readObject();
            Log.i(SimpleDynamoActivity.TAG, "ACK read: " + pojoAck.asString());
            is.close();
            ois.close();

            stream.close();
            oos.close();
            socket.close();

        } catch (SocketTimeoutException e) {
            Log.e("SOCKET TIMEOUT","failed to get ACK " + pojo.asString(),e);
            return false;
        } catch (Exception e) {
            Log.e(SimpleDynamoActivity.TAG,"exception" + e.getMessage(),e);
            return false;
        }
        return true;
    }
    public Pojo startMachineTask(Pojo pojo) {
        /*
            Increment sequence counter.
         */
        try {
            ListeningServerRunnable.requestLock.acquire();
            pojo.setRequestSequence(ListeningServerRunnable.requestSequence);
            Log.i("SPINNING AND WAITING", "beginning request for " + pojo.asString());
            ListeningServerRunnable.requestList.add(pojo);
            ListeningServerRunnable.requestSequence++;
            ListeningServerRunnable.requestLock.release();

            if (ProviderHelper.getInstance().sendPojoToDestinationPort(pojo)) {
                Log.i("RESPONSE_SUCCESS","successfully sent message and received ack: " + pojo.asString());
            } else {
                Log.e("RESPONSE_ERROR","failed to get response " + pojo.asString());
            }
            spinUntilResponse(pojo.getRequestSequence());

        } catch (InterruptedException e) {
           Log.e("EXCEPTION,",e.getMessage(),e);
        }
        return ListeningServerRunnable.requestList.get(pojo.getRequestSequence());
    }

    public void spinUntilResponse(int sequenceNumber) {
        boolean forever = true;
        try {
            while (forever) {

                ListeningServerRunnable.requestLock.acquire();
                ListeningServerRunnable.requestList.get(sequenceNumber);
                if (ListeningServerRunnable.requestList.get(sequenceNumber).getType() == Pojo.TYPE_RESPONSE) {
                    Log.i("RESPONSE","received response");
                    forever = false;
                }
                ListeningServerRunnable.requestLock.release();
                //Log.i("SPINNING AND WAITING ",ListeningServerRunnable.requestList.get(sequenceNumber).asString());
                Thread.currentThread().yield();
            }
            Log.i("SPINNING_AND_WAITING","no longer spinning, response received for sequence " + sequenceNumber);
    } catch (InterruptedException e) {
        Log.e("EXCEPTION,",e.getMessage(),e);
    }
    }
}
