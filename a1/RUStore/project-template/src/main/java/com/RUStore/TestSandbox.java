package com.RUStore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

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

		/* Test with a file */
		String fileKey = "chapter1.txt";
		String inputPath = "./inputfiles/temp";
		String outputPath = "./outputfiles/temp_";
		for(int i=0;i<4;i++)
		{
			if(i==1)
			{
				fileKey = "chapter2.txt";
				inputPath = "./inputfiles/dummysite.html";
				outputPath= "./outputfiles/dummysite_.html";
			}
			else if(i==2)
			{
				fileKey = "chapter3.txt";
				inputPath = "./inputfiles/knight.jpg";
				outputPath= "./outputfiles/knight_.jpg";
			}
			else if(i==3)
			{
				fileKey = "chapter4.txt";
				inputPath = "./inputfiles/solutions.pdf";
				outputPath= "./outputfiles/solutions_.pdf";
			}
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

			// GET File
			try {
				System.out.println("Getting object with key \"" + fileKey + "\"");
				int ret = client.get(fileKey, outputPath);
				if(ret == 0) {
					File fileIn = new File(inputPath);
					File fileOut = new File(outputPath);
					if(fileOut.exists()) {
						byte[] fileInBytes = Files.readAllBytes(fileIn.toPath());
						byte[] fileOutBytes = Files.readAllBytes(fileOut.toPath());
						if(Arrays.equals(fileInBytes, fileOutBytes)) {
							System.out.println("File contents are equal! Successfully Retrieved File");
						}else {
							System.out.println("File contents are not equal! Got garbage data. (BAD FILE DOWNLOAD)");
						}
						System.out.println("Deleting downloaded file.");
						Files.delete(fileOut.toPath());
					}else {
						System.out.println("No file downloaded. (BAD FILE DOWNLOAD)");
					}
				}else {
					System.out.println("Failed getting object with key \"" + fileKey + "\". Key doesn't exist. (INCORRECT RETURN)");
				}
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("Failed getting object with key \"" + fileKey + "\" Exception occured.");
			}
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
