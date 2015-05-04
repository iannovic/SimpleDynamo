package edu.buffalo.cse.cse486586.simpledynamo.data;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

import java.sql.SQLException;

/**
 * Created by ianno_000 on 2/24/2015.
 */

public class DynamoDAO {

    /*
        based off of the DAO from http://www.vogella.com/tutorials/AndroidSQLite/article.html
     */
    private SQLiteDatabase db;
    private DatabaseHelper helper;

    private String[] columns = {DatabaseHelper.TABLE_COLUMN_ID, DatabaseHelper.TABLE_COLUMN_MESSAGE};

    public DynamoDAO(Context context) {
        helper = new DatabaseHelper(context);
    }

    public void open() throws SQLException {
        db = helper.getWritableDatabase();
    }
    public void close() {
        helper.close();
    }

    public Long insertMessage(ContentValues values) {

        Long newId = db.replace(DatabaseHelper.TABLE_NAME, "", values);

        if (newId > -1) {
            String delimiter = DatabaseHelper.TABLE_COLUMN_ID + " = ?";
            Cursor cursor = db.query(DatabaseHelper.TABLE_NAME, columns, delimiter, new String[]{(String)values.get("key")}, null, null, null);
            if (cursor != null && cursor.moveToFirst()) {
               Log.v("CSE486",cursor.getString(cursor.getColumnIndex("key")) + ":" + cursor.getString(cursor.getColumnIndex("key")));
            } else {
                Log.e("CSE486","failed to insert");
            }
            cursor.close();
            db.close();
        }
        else
        {
            Log.e("CSE486","failed to insert message");
        }
        return newId;
    }

    public boolean recreateDatabase() {
        helper.onUpgrade(db, DatabaseHelper.DATABASE_VERSION, DatabaseHelper.DATABASE_VERSION);
        return true;
    }
    public Cursor queryMessage(String selection) {

        Cursor cursor = null;

        if (selection.equals(DatabaseHelper.LOCAL_KEY) || selection.equals(DatabaseHelper.GLOBAL_KEY)) {
            Log.i("DAO_QUERY","ALL");
           cursor =  db.query(DatabaseHelper.TABLE_NAME, DatabaseHelper.TABLE_PROJECTION,null,null,null,null,null);
        } else {
            Log.i("DAO_QUERY","SINGLE: " + selection);
            String selectClause = "key = ?";
            cursor = db.query(DatabaseHelper.TABLE_NAME, DatabaseHelper.TABLE_PROJECTION,selectClause,new String[]{selection},null,null,null);
        }

        return cursor;
    }

    public int deleteMessage(String selection, String[] selectionArgs) {

        int ret = 0;

        if (selection.equals(DatabaseHelper.LOCAL_KEY) || selection.equals(DatabaseHelper.GLOBAL_KEY)) {
            Log.i("DAO_DELETE","ALL");
           ret = db.delete(DatabaseHelper.TABLE_NAME,null,null);
        } else {
            Log.i("DAO_DELETE",selection);
            String selection2 = "key = ?";
            ret = db.delete(DatabaseHelper.TABLE_NAME,selection2,new String[]{selection});
        }

        return ret;
    }
}
