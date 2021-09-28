package com.RUStore;

import java.io.IOException;

/**
 * This TestSandbox is meant for you to implement and extend to 
 * test your object store as you slowly implement both the client and server.
 * 
 * If you need more information on how an RUStorageClient is used
 * take a look at the RUStoreClient.java source as well as 
 * TestSample.java which includes sample usages of the client.
 */
public class TestSandbox{
	public static void main(String[] args) throws IOException{

		// Create a new RUStoreClient
		RUStoreClient client = new RUStoreClient("localhost", 9000);

		// Open a connection to a remote service
		System.out.println("Connecting to object server...");
		try {
			client.connect();
			System.out.println("Established connection to server.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to connect to server.");
		}

		String fileKey = "chapter1.txt";
		String inputPath = "./inputfiles/solutions.pdf";
		// String outputPath = "../outputfiles/lofi_.mp3";

		// PUT File
		try {
			System.out.println("Putting file \"" + inputPath + "\" with key \"" + fileKey + "\"");
			int ret = client.put(fileKey, inputPath);
			if(ret == 0) {
				System.out.println("Successfully put file!");
			}else {
				System.out.println("Failed to put file \"" + inputPath + "\" with key \"" + fileKey + "\". Key already exists. (INCORRECT RETURN)");
			}
		} catch (Exception e1) {
			e1.printStackTrace();
			System.out.println("Failed to put file \"" + inputPath + "\" with key \"" + fileKey + "\". Exception occured.");
		} 

		System.out.println("Attempting to disconnect...");
		try {
			client.disconnect();
			System.out.println("Sucessfully disconnected.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to disconnect.");
		}
	}

}
