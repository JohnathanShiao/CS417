package com.RUStore;

/* any necessary Java packages here */
import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;

public class RUStoreServer {

	/* any necessary class members here */
	private static ServerSocket serverSocket;
	private static Socket clientSocket;
	private static PrintWriter out;
	private static BufferedReader in;
	private static HashMap<String,byte[]> storage;
	/* any necessary helper methods here */

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

	public static void serv_post() throws IOException
	{
		//ACK the request
		out.println("-");
		String response, key;
		//get key
		response = in.readLine();
		if(storage.containsKey(response))
		{
			//send = if key already exists, don't need to continue
			out.println("=");
			return;
		}
		else
		{	
			//send - if key does not exist
			out.println("-");
			key = response;
			System.out.printf("Putting \"%s\"\n",key);
		}
		//get data
		response=in.readLine();
		//convert from toString to byte[]
		byte[] temp = convert(response);
		//place into storage
		storage.put(key, temp);
	}

	public static void serv_remove() throws IOException
	{
		//ACK the request
		out.println("-");
		String response;
		//get key
		response=in.readLine();
		if(storage.containsKey(response))
		{
			System.out.printf("removing \"%s\"\n",response);
			storage.remove(response);
			out.println("-");	
		}
		else{
			//key was not found
			out.println("!");
		}

	}

	public static void serv_get() throws IOException
	{
		//ACK the request
		out.println("-");
		String response, key;
		//get key
		response = in.readLine();
		if(storage.containsKey(response))
		{
			//send -, key exists, sending data
			out.println("-");
			key = response;
			System.out.printf("getting \"%s\"\n",key);
			byte[] result = storage.get(key);
			out.println(Arrays.toString(result));
		}
		else
		{	
			//send ! if key does not exist
			out.println("!");
			return;
		}
	}

	public static void serv_list() throws IOException
	{
		//ACK the request
		out.println("-");
		System.out.println("Sending list of keys");
		if(storage.isEmpty())
		{
			//no keys, tell client
			out.println("!");
		}
		else
		{
			//there are keys, sending
			out.println("-");
			out.println(storage.keySet());
			return;
		}
	}
	public static void handle_connection() throws IOException
	{
		while(true)
		{
			String command = in.readLine();
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
				serverSocket.close();
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
		//accept a client
		clientSocket = serverSocket.accept();
		System.out.println("Connection established.");
		out = new PrintWriter(clientSocket.getOutputStream(), true);
		in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		storage = new HashMap<String, byte[]>();
		//send to helper to resolve
		handle_connection();
		return;
	}

}
