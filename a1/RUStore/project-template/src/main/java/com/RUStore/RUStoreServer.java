package com.RUStore;

/* any necessary Java packages here */
import java.io.*;
import java.net.*;
import java.util.HashMap;

public class RUStoreServer {

	/* any necessary class members here */
	private static ServerSocket serverSocket;
	private static Socket clientSocket;
	private static DataOutputStream out;
	private static DataInputStream in;
	private static HashMap<String,byte[]> storage;
	/* any necessary helper methods here */

	public static void serv_post() throws IOException
	{
		//ACK the request
		out.writeUTF("-");
		String response, key;
		//get key
		response = in.readUTF();
		if(storage.containsKey(response))
		{
			//send = if key already exists, don't need to continue
			out.writeUTF("=");
			return;
		}
		else
		{	
			//send - if key does not exist
			out.writeUTF("-");
			key = response;
			System.out.printf("Putting \"%s\"\n",key);
		}
		int len = in.readInt();
		byte[] temp = in.readNBytes(len);
		//place into storage
		storage.put(key, temp);
	}

	public static void serv_remove() throws IOException
	{
		//ACK the request
		out.writeUTF("-");
		String response;
		//get key
		response=in.readUTF();
		if(storage.containsKey(response))
		{
			System.out.printf("removing \"%s\"\n",response);
			storage.remove(response);
			out.writeUTF("-");	
		}
		else{
			//key was not found
			out.writeUTF("!");
		}

	}

	public static void serv_get() throws IOException
	{
		//ACK the request
		out.writeUTF("-");
		String response, key;
		//get key
		response = in.readUTF();
		if(storage.containsKey(response))
		{
			//send -, key exists, sending data
			out.writeUTF("-");
			key = response;
			System.out.printf("getting \"%s\"\n",key);
			byte[] result = storage.get(key);
			int len = result.length;
			out.writeInt(len);
			out.write(result);
		}
		else
		{	
			//send ! if key does not exist
			out.writeUTF("!");
			return;
		}
	}

	public static void serv_list() throws IOException
	{
		//ACK the request
		out.writeUTF("-");
		System.out.println("Sending list of keys");
		if(storage.isEmpty())
		{
			//no keys, tell client
			out.writeUTF("!");
		}
		else
		{
			//there are keys, sending
			out.writeUTF("-");
			out.writeUTF(storage.keySet().toString());
			return;
		}
	}
	public static void handle_connection() throws IOException
	{
		while(true)
		{
			String command = in.readUTF();
			if(command.equals("POST"))
			{
				serv_post();
			}
			else if (command.equals("REM"))
			{
				serv_remove();
			}
			else if (command.equals("GET"))
			{
				serv_get();
			}
			else if (command.equals("LIS"))
			{
				serv_list();
			}
			else if (command.equals("DIS"))
			{
				System.out.println("Server: Client has disconnected.");
				//close sockets
				clientSocket.close();
				return;
			}
		}
	}
	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 */
	public static void main(String args[])throws IOException{

		// Check if at least one argument that is potentially a port number
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);

		// Implement here //
		//create server
		serverSocket = new ServerSocket(port);
		System.out.printf("Server started. Listening on port %d.\n",port);
		//persists storage as per discussion
		storage = new HashMap<String, byte[]>();
		while(true)
		{
			clientSocket = serverSocket.accept();
			System.out.println("Connection established.");
			out = new DataOutputStream(clientSocket.getOutputStream());
			in = new DataInputStream(clientSocket.getInputStream());
			//send to helper to resolve
			handle_connection();
		}
	}

}
