package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static Integer[] ports = {Constants.PORT1, Constants.PORT2, Constants.PORT3, Constants.PORT4, Constants.PORT5};
	String failedNode = "";	//stores any failed node
	String failedInsertions = "";	// stores any failed insertions due to node failure
	String deletedFiles = "";		//stores any failed deletions - to handle node failure


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private int findCoordinator(String key){
		int  coordinator = 0;
		int coordinatorIndex = 0;
		try{

			String hashKey = genHash(key);
			for(int i=0;i<ports.length;i++){

				if(hashKey.compareTo(genHash(Integer.toString(ports[i]))) <= 0){
					coordinator = ports[i];
					coordinatorIndex = i;
					break;
				}
			}
			if(coordinator == 0){
				coordinator = ports[0];
				coordinatorIndex = 0;
			}
		}catch (NoSuchAlgorithmException ex){
			Log.e("exception", "no such algo exception from client for key"+key);
		}

		return coordinatorIndex;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {

		String key = values.getAsString("key");
		String value = values.getAsString("value");
		//find the coordinator device based on the hashed key value
		int coordinatorIndex = findCoordinator(key);
		for(int i=0; i<3;i++){
			int portIndex = (coordinatorIndex+i)%5;
			Message message = new Message(Constants.INSERT, key, value);
			try{
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						ports[portIndex]*2);
				socket.setSoTimeout(500);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				BufferedReader br3 = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				out.writeObject(message);
				String ack = br3.readLine();
				if(ack.split("\\|").length == 0);
				br3.close();
				out.close();
				socket.close();
			}catch (SocketTimeoutException ex){
				//save the failed node and insert value details
				failedNode = Integer.toString(ports[portIndex]);
				failedInsertions = (failedInsertions.isEmpty())?key+">"+value:failedInsertions + "|"+ key+">"+value;
			}
			catch (NullPointerException ex){
				//save the failed node and insert value details
				failedNode = Integer.toString(ports[portIndex]);
				failedInsertions = (failedInsertions.isEmpty())?key+">"+value:failedInsertions + "|"+ key+">"+value;
			}catch (UnknownHostException e) {
				Log.e("exception", "InsertTask UnknownHostException");
			} catch (IOException e) {
				Log.e("exception", "InsertTask socket IOException");
			}catch(Exception ex){
				Log.e("exception", "InsertTask exception "+ex);
			}
		}
		return uri;
	}
	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// Method to delete the files in internal storage
		try {
			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

			if (selection.equals(Constants.LOCALQ)) {	//delete all the files in requested device
				localDelete();

			}else{
				if(!selection.equals(Constants.GLOBALQ)){	// delete all the files in all the devices
					deleteRecord(selection);
				}else {
					localDelete();
				}
				Log.e("deleted","delete calling client "+ myPort);

			}
		}
		catch ( Exception ex){
			Log.e("exception", "caught exception in delete "+ex);
		}
		return 0;
	}
	public int localDelete(){
		int count = 0;
		try{
			File file = new File(String.valueOf(getContext().getFilesDir()));
			File[] internalFiles = file.listFiles();
			for(int i=0;i < internalFiles.length;i++){
				deleteRecord(internalFiles[i].getName());
				count++;
			}

		}catch(Exception ex){
			Log.e("exception", "caught exception in localDelete method "+ex);
		}
		return count;
	}
	public void deleteRecord(String key){
		try {
			File dir = getContext().getFilesDir();
			File file = new File(dir, key);
			file.delete();
			deletedFiles = (deletedFiles.isEmpty())?key:deletedFiles+"|"+key;
		}catch(Exception e){
			Log.e("exception","exception at deleteRecord()"+e);
		}
	}

	private void sortPorts(){
		Arrays.sort(ports, new Comparator<Integer>(){
			@Override
			public  int compare(Integer one, Integer two){
				int returnVal = 0;
				try{
					returnVal = genHash(Integer.toString(one)).compareTo(genHash(Integer.toString(two)));
				}
				catch(NoSuchAlgorithmException ex){
					Log.e("exception","no such algo exception in : findSuccessors()");
				}
				return returnVal;
			}
		});
	}
	@Override
	public boolean onCreate() {
		sortPorts();
		try{
			ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}catch (IOException e) {

			Log.e("exception", e.getMessage());

		}
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr);

		return false;
	}
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		String[] colNames = {"key", "value"};
		MatrixCursor data = new MatrixCursor(colNames);
		Message message = new Message(Constants.QUERY, selection, "");
		try{
			String response = "";

			if (selection.equals(Constants.LOCALQ)){			//local query
				response = localQuery();
			}else if(!selection.equals(Constants.GLOBALQ)){  //key based retrieval
				int coordinatorIndex = findCoordinator(selection);

				for(int i=0; i<3;i++) {
					int portIndex = (coordinatorIndex+i)%5;
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								ports[portIndex]*2);
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						out.writeObject(message);
						String ack = br.readLine();
						if(ack.split("\\|").length == 0);
						response = ack;
						out.close();
						br.close();
						socket.close();
						break;

					} catch (UnknownHostException e) {
						Log.e("exception", "QueryTask UnknownHostException");

					} catch (IOException e) {
						Log.e("exception", "QueryTask socket IOException");

					} catch (NullPointerException ex){
						Log.e("exception", "QueryTask failed at "+ports[portIndex]);
					}
				}
			}else{			//retrieve all - *

				String reply = "";


				for(int i=0;i<ports.length;i++){
					String ack = "";
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								ports[i]*2);
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						out.writeObject(message);
						ack = br.readLine();
						if(ack.split("\\|").length == 0);
						out.close();
						br.close();
						socket.close();
						reply = (reply.isEmpty())?ack: reply+"*"+ack;

					} catch (UnknownHostException e) {
						Log.e("exception", "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e("exception", "ClientTask socket IOException");
					} catch (NullPointerException e) {
						Log.e("exception", "NullPointerException " + ports[i]+" -- "+e);
					}

				}
				response = reply;
			}
			if (!response.isEmpty()) {
				String[] records = response.split("\\*");
				for (String rec : records) {
					String[] record = rec.split("\\>");
					if(record != null && record.length >= 2) {
						data.addRow(new Object[]{record[0], record[1]});
					}

				}

			}

		}catch (Exception ex){
			Log.e("query", "caught exception: "+ex);
		}

		return data;
	}

	public String localQuery(){

		String records = "";
		try{
			File file = new File(String.valueOf(getContext().getFilesDir()));
			File[] internalFiles = file.listFiles();
			for(int i=0;i < internalFiles.length;i++){
				if(internalFiles[i] != null) {
					String rec = retrieveLocalRecord(internalFiles[i].getName());
					records = (records.isEmpty()) ? rec : records + "*" + rec;
				}
			}

		}catch(Exception ex){
			Log.e("exception", "caught exception in localQuery method "+ex);
		}

		return records;
	}
	public String retrieveLocalRecord(String key){
		String records = "";
		try {
			FileInputStream inputStream = getContext().openFileInput(key);
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			String value = "";
			if((value = reader.readLine()) != null){
				records = key+">"+value;
			}
			inputStream.close();

		}
		catch (FileNotFoundException e) {
			Log.e("exception","query - no such entry");
		} catch (IOException e) {
			Log.e("exception","query - io exception");
		}
		return records;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		public synchronized String queryStar(){

			String records = "";
			try{

				File file = new File(String.valueOf(getContext().getFilesDir()));
				File[] internalFiles = file.listFiles();
				for(int i=0;i < internalFiles.length;i++){
					if(internalFiles[i] != null) {
						String rec = retrieveLocalRecord(internalFiles[i].getName());
						records = (records.isEmpty()) ? rec : records + "*" + rec;
					}
				}

			}catch(Exception ex){
				Log.e("exception", "caught exception in localQuery method "+ex);
			}

			return records;
		}
		public synchronized String queryKeyBased(String key){
			String records = "";
			try {
				FileInputStream inputStream = getContext().openFileInput(key);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String value = "";
				if((value = reader.readLine()) != null){
					records = key+">"+value;
				}
				inputStream.close();

			}
			catch (FileNotFoundException e) {
				Log.e("exception","query - no such entry");
			} catch (IOException e) {
				Log.e("exception","query - io exception");
			}
			return records;
		}

		public synchronized void insertHere(String key, String value){
			try {
				FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.close();
			}
			catch(Exception ex){
				Log.e("exception", "caught exception insertHere() server : " + ex);
			}
		}

		public synchronized String recoverData(String failedAvd){
			String reply = "";
			if(failedNode.equals(failedAvd)){
				reply = failedInsertions+ "*"+ deletedFiles;
				failedInsertions = "";
				deletedFiles = "";
				failedNode = "";
			}
			return reply;
		}

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while(true){
				try{
					Socket clSocket = serverSocket.accept();
					ObjectInputStream in = new ObjectInputStream(clSocket.getInputStream());
					PrintWriter pr = new PrintWriter(clSocket.getOutputStream());
					Message message = null;
					String reply = "";
					message = (Message) in.readObject();
					if(message != null){
						if(message.isRequest(Constants.INSERT)){
							insertHere(message.getKey(), message.getValue());
						}else if(message.isRequest(Constants.QUERY)){
							if(message.getKey().equals(Constants.GLOBALQ)){
								reply = queryStar();

							}else{
								reply = queryKeyBased(message.getKey());
							}
						}else if(message.isRequest(Constants.RECOVERY)){
							reply = recoverData(Integer.toString(message.getSender()));
						}
					}
					pr.println(reply);
					pr.flush();
					pr.close();
					in.close();
					clSocket.close();

				}
				catch (Exception e){
					Log.e("exception","Exception on server: "+e);
				}

			}

		}


		protected void onProgressUpdate(String...msgs) {
			return;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		public synchronized void saveHereValues(String missedFiles){
			String[] records = missedFiles.split("\\|");
			for(String rec: records){
				String[] val = rec.split("\\>"); // 0 - key, 1 - value
				if(!deletedFiles.contains(val[0])){
					try {
						FileOutputStream outputStream = getContext().openFileOutput(val[0], Context.MODE_PRIVATE);
						outputStream.write(val[1].getBytes());
						outputStream.close();
					}
					catch(Exception ex){
						Log.e("exception", "caught exception in saveHereValues() " + ex);
					}
				}
			}
		}

		@Override
		protected Void doInBackground(String... msgs) {

			String missedFiles = "";
			String deletedFiles = "";
			try{
				String currentAvd = msgs[0];
				Message message = new Message(Constants.RECOVERY, Integer.parseInt(currentAvd));
				for(int i = 0; i<ports.length;i++){
					if(ports[i].equals(currentAvd)){
						continue;
					}
					try{
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								ports[i]*2);
						String ack = "";
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						out.writeObject(message);
						ack = br.readLine();	//key>val|key>val (inserted) * key|key (deleted)
						out.close();
						br.close();
						if(ack.isEmpty()){
							continue;
						}
						String[] files = ack.split("\\*"); // 0 - insertions, 1 - failure

						if(files != null) {
							if (files.length >= 2) {
								missedFiles = (missedFiles.isEmpty()) ? files[0] : missedFiles + "|" + files[0];
								deletedFiles = (deletedFiles.isEmpty()) ? files[1] : deletedFiles + "|" + files[1];
							} else if (files.length == 1) {
								missedFiles = (missedFiles.isEmpty()) ? files[0] : missedFiles + "|" + files[0];
							}
						}
						socket.close();

					}catch(NullPointerException ex){
						Log.e("exception", "null ptr exception at"+ports[i]);
					}


				}
				if(!missedFiles.isEmpty()){
					saveHereValues(missedFiles);
				}

			}catch (Exception ex){
				Log.e("failure", "exception from client "+msgs[1]+" - "+ex);
			}

			return null;
		}
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
