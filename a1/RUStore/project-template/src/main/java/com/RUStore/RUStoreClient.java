package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.io.*;

public class RUStoreClient {

	/* any necessary class members here */
	private String ip;
	private int port_num;
	private Socket clientSocket;
	private PrintWriter out;
	private BufferedReader in;
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
		out = new PrintWriter(clientSocket.getOutputStream(), true);
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));	
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
		out.println("POST");
		//Waiting for an ack with '-'
		response = in.readLine();
		if(response.equals("-"))
		{
			//got an ack so continue
			out.println(key);
			response=in.readLine();
			if(response.equals("="))
			{
				//key already exists, return 1
				return 1;
			}
			out.println(Arrays.toString(data));
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
	 * Converts a byte[] that has had Arrays.toString called on it back into
	 * a byte[]
	 * @param byteString
	 * @return			the original byte[]
	 */
	public static byte[] convert(String byteString)
	{
		//remove brackets, convert to array
		String[] str = byteString.substring(1,byteString.length()-1).split(",");
		byte[] temp = new byte[str.length];
		for(int i=0;i<str.length;i++)
		{
			temp[i] = Byte.parseByte(str[i].trim());
		}
		return temp;
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
		out.println("GET");
		//get ACK
		response = in.readLine();
		if(response.equals("-"))
		{
			//send key wanted
			out.println(key);
			response = in.readLine();
			//if - then key exists
			if(response.equals("-"))
			{
				response = in.readLine();
				return convert(response);
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
		if(outputFile.createNewFile())
		{
			Files.write(outputFile.toPath(),data);
			return 0;	
		}
		else{
			System.out.println("File already exists");
		}
		throw new IOException();
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
		out.println("REM");
		//Waiting for an ack with '-'
		response = in.readLine();
		if(response.equals("-"))
		{
			//got an ack so continue
			out.println(key);
			response=in.readLine();
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
		out.println("LIS");
		//get ACK
		response = in.readLine();
		if(response.equals("-"))
		{
			//read key list
			response = in.readLine();
			if(response.equals("-"))
			{
				response = in.readLine();
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
		out.println("DIS");
		clientSocket.close();
	}

}
