package edu.buffalo.cse.cse486586.simpledynamo.data;

import android.content.ContentValues;
import android.util.Log;

import java.io.Serializable;

/**
 * Created by ianno_000 on 4/25/2015.
 */
public class Pojo implements Serializable {

    public static final int TYPE_ACK = 0;
    public static final int TYPE_COORDINATOR_INSERT = 1;
    public static final int TYPE_NODE_INSERT = 2;
    public static final int TYPE_COORDINATOR_QUERY = 3;
    public static final int TYPE_NODE_QUERY = 4;
    public static final int TYPE_RESPONSE = 5;

    private int type;
    private int sendingVersion;
    private int requestSequence;
    private String destinationPort;
    private String sendingPort;
    private String key;
    private String value;

    public int getRequestSequence() {
        return requestSequence;
    }

    public void setRequestSequence(int requestSequence) {
        this.requestSequence = requestSequence;
    }

    public void setValues(ContentValues values) {
        key = (String) values.get("key");
        value = (String) values.get("value");
    }
    public ContentValues getValues() {

        ContentValues values = new ContentValues();
        values.put("key",key);
        values.put("value",value);

        if (type == TYPE_COORDINATOR_INSERT) {
            values.put("coordinator","coordinator");
        } else if (type == TYPE_NODE_INSERT) {
            values.put("node","node");
        }

        return values;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getDestinationPort() {
        return destinationPort;
    }

    public void setDestinationPort(String destinationPort) {
        this.destinationPort = destinationPort;
    }

    public String getSendingPort() {
        return sendingPort;
    }

    public void setSendingPort(String sendingPort) {
        this.sendingPort = sendingPort;
    }

    public int getSendingVersion() {
        return sendingVersion;
    }

    public void setSendingVersion(int sendingVersion) {
        this.sendingVersion = sendingVersion;
    }

    public void print() {
        Log.i("POJO","{ " + type + ", " + sendingVersion + ", " + destinationPort + ", " + sendingPort + ", " + key + ", " + value + "}");
    }

    public String asString() {
        return "{ " + type + ", " + sendingVersion + ", " + requestSequence + ", " + destinationPort + ", " + sendingPort + ", " + key + ", " + value + "}";
    }
}
