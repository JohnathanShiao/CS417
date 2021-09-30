package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.nio.file.Files;
import java.io.*;

public class RUStoreClient {

	/* any necessary class members here */
	private String ip;
	private int port_num;
	private Socket clientSocket;
	private DataOutputStream out;
	private DataInputStream in;
	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) throws IOException{
		// Implement here
		ip = host;
		port_num = port;
	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void connect() throws UnknownHostException, IOException{

		// Implement here
		clientSocket = new Socket(ip,port_num);
		out = new DataOutputStream(clientSocket.getOutputStream());
		in = new DataInputStream(clientSocket.getInputStream());	
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 */
	public int put(String key, byte[] data) throws IOException{

		// Implement here
		String response;
		//Similar to POST from HTTP because we don't update if the key exists
		out.writeUTF("POST");
		//Waiting for an ack with '-'
		response = in.readUTF();
		if(response.equals("-"))
		{
			//got an ack so continue
			out.writeUTF(key);
			response=in.readUTF();
			if(response.equals("="))
			{
				//key already exists, return 1
				return 1;
			}
			int len = data.length;
			out.writeInt(len);
			out.write(data);
			return 0;
		}
		throw new IOException();
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 */
	public int put(String key, String file_path) throws IOException{

		// Implement here
		File file = new File(file_path);
		//change a file into a byte[]
		byte[] data = Files.readAllBytes(file.toPath());
		return put(key,data);
	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 */
	public byte[] get(String key) throws IOException{

		// Implement here
		String response;
		//send command
		out.writeUTF("GET");
		//get ACK
		response = in.readUTF();
		if(response.equals("-"))
		{
			//send key wanted
			out.writeUTF(key);
			response = in.readUTF();
			//if - then key exists
			if(response.equals("-"))
			{
				int len = in.readInt();
				byte[] temp = in.readNBytes(len);
				return temp;
			}
			else{
				return null;
			}
		}
		throw new IOException();

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 */
	public int get(String key, String file_path) throws IOException{

		// Implement here
		byte[] data = get(key);
		if(data == null)
		{
			//key did not exist
			return 1;
		}
		File outputFile = new File(file_path);
		try{
			FileOutputStream outStream = new FileOutputStream(outputFile,false);
			outStream.write(data);
			outStream.close();
			return 0;
		}catch(FileNotFoundException e)
		{
			throw new IOException();
		}
		
	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 */
	public int remove(String key) throws IOException{

		// Implement here
		String response;
		//Similar to POST from HTTP because we don't update if the key exists
		out.writeUTF("REM");
		//Waiting for an ack with '-'
		response = in.readUTF();
		if(response.equals("-"))
		{
			//got an ack so continue
			out.writeUTF(key);
			response=in.readUTF();
			if(response.equals("-"))
			{
				return 0;
			}
			else{
				return 1;
			}
		}
		throw new IOException();

	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 */
	public String[] list() throws IOException{

		// Implement here
		String response;
		//send command
		out.writeUTF("LIS");
		//get ACK
		response = in.readUTF();
		if(response.equals("-"))
		{
			//key check
			response = in.readUTF();
			if(response.equals("-"))
			{
				//read list
				response = in.readUTF();
				response = response.substring(1,response.length()-1);
				return response.split(",");
			}
			else{
				//no keys
				return null;
			}
		}
		throw new IOException();

	}

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void disconnect() throws IOException{

		// Implement here
		out.writeUTF("DIS");
		clientSocket.close();
	}

}
