package cs555.erasure.node;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;

import cs555.erasure.transport.TCPReceiverThread;
import cs555.erasure.transport.TCPSender;
import cs555.erasure.transport.TCPServerThread;
import cs555.erasure.util.NodeInformation;
import cs555.erasure.wireformats.ClientChunkServerRequestToController;
import cs555.erasure.wireformats.ClientReadFileRequestToController;
import cs555.erasure.wireformats.ClientRegisterRequestToController;
import cs555.erasure.wireformats.ClientRequestToReadFromChunkServer;
import cs555.erasure.wireformats.ClientSendChunkToChunkServer;
import cs555.erasure.wireformats.ControllerChunkServerToReadResponseToClient;
import cs555.erasure.wireformats.ControllerChunkServersResponseToClient;
import cs555.erasure.wireformats.ControllerRegisterResponseToClient;
import cs555.erasure.wireformats.Event;
import cs555.erasure.wireformats.Protocol;
import cs555.erasure.wireformats.ChunkServerSendChunkToClient;
import erasure.ReedSolomon;

/**
 * Client which is responsible for storing, retrieving, and updating files in the system. The client
 * is responsible for splitting a file into chunks and assembling the file back using chunks during
 * retrieval.
 */

public class Client implements Node {

	private static boolean DEBUG = false;
	private NodeInformation controllerNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	private TCPReceiverThread clientTCPReceiverThread;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender controllerSender;
	private ArrayList<byte[]> fileIntoChunks;
	private NodeInformation clientNodeInformation;
	private HashMap<String, HashMap<Integer, HashMap<Integer, NodeInformation>>> fileWithChunkNumberWithShardWithChunkServers;
	private HashMap<Integer, byte[]> locallyStoredShardsWithBytes;
	private HashMap<String, HashMap<Integer, byte[]>> receivedChunksMap;
	private HashMap<String, HashMap<Integer, HashMap<Integer, NodeInformation>>> receivedFileWithChunkNumberWithShardWithChunkServers;
	private HashMap<Integer, HashMap<Integer, byte[]>> receivedFileWithChunkNumberWithShardWithBytes;
	private static final int DATA_SHARDS = 6;
	private static final int PARITY_SHARDS = 3;
	private static final int TOTAL_SHARDS = 9;
	private static final int BYTES_IN_INT = 4;
	private static final int SIZE_OF_CHUNK = 1024 * 64;
	private static Client client;
	
	private Client(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.locallyStoredShardsWithBytes = new HashMap<Integer, byte[]>();
		this.receivedChunksMap = new HashMap<String, HashMap<Integer, byte[]>>();
		this.fileWithChunkNumberWithShardWithChunkServers = new HashMap<String, HashMap<Integer, HashMap<Integer, NodeInformation>>>();
		this.receivedFileWithChunkNumberWithShardWithChunkServers = new HashMap<String, HashMap<Integer, HashMap<Integer, NodeInformation>>>();
		this.receivedFileWithChunkNumberWithShardWithBytes = new HashMap<Integer, HashMap<Integer, byte[]>>();
		
		try {
			TCPServerThread serverThread = new TCPServerThread(0, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			//this.accessUserInput = true;
			this.fileIntoChunks = new ArrayList<byte[]>();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		this.clientNodeInformation = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
		// Once the initialization is complete, client should send a registration request to the controller.
		connectToController();
	}
	
	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event " + eventType + " Passed to Client."); }
		switch(eventType) {
			// CONTROLLER_REGISTER_RESPONSE_TO_CLIENT = 6001
			case Protocol.CONTROLLER_REGISTER_RESPONSE_TO_CLIENT:
				handleControllerRegisterResponse(event);	
				break;
			// CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT = 6002
			case Protocol.CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT:
				handleControllerChunkServersResponse(event);
				break;
			// CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT = 6003
			case Protocol.CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT:
				handleControllerChunkServerToReadResponseToClient(event);
				break;
			// CHUNKSERVER_SEND_CHUNK_TO_CLIENT = 7002
			case Protocol.CHUNKSERVER_SEND_CHUNK_TO_CLIENT:
				ChunkServerSendChunkToClient(event);
				break;
			default:
				System.out.println("Invalid Event to Node.");
				return;
		}
	}

	@Override
	public void setLocalHostPortNumber(int localPort) {
		this.localHostPortNumber = localPort;
	}
	
	public static void main(String[] args) {
		
		// requires 2 arguments to initialize a node
		if(args.length != 2) {
            System.out.println("Invalid Arguments. Must include host name and port number.");
            return;
        }
		
		// testing for debugging, assuming that the IP address and arguments are valid commands
		if(DEBUG) {
			System.out.println("In Debug Mode.");
			try {
				System.out.println("My address is: " + InetAddress.getLocalHost().getCanonicalHostName());
			} catch (UnknownHostException uhe) {
				uhe.printStackTrace();
			}
		}
		
		String controllerIPAddress = args[0];
		int controllerPortNumber = 0;
		
		try {
			controllerPortNumber = Integer.parseInt(args[1]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Second argument must be a number.");
			nfe.printStackTrace();
		}
		
		Client client = new Client(controllerIPAddress, controllerPortNumber);
		
		client.handleUserInput();
	}
	
	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Ready for input.");
			
        while(true) {
        	//if (this.accessUserInput) {
	            System.out.println("Options:\n[S] Store a File\n[R] Read a File\n[Q] Quit\nPlease type your request: ");
	            String input = scan.nextLine();
	            
	            input = input.toUpperCase();
	            switch (input) {
	            	case "S":
	            		if (DEBUG) { System.out.println("User selected Store a file."); }
	            		String filename;
	            		System.out.println("Enter the name of the file that you wish to store: ");
						filename = scan.nextLine();
						File file = new File(filename);
						if (file.exists()) {
							this.fileIntoChunks = splitFileIntoBytes(file);
							int numberOfChunks = fileIntoChunks.size();
							int chunkNumber = 0;
							sendClientChunkServerRequestToController(chunkNumber, filename, numberOfChunks);
						} else {
							System.out.println("File does not exist. Please try again.");
						}
	            		break;
	            	case "R":
	            		if (DEBUG) { System.out.println("User selected Read a file."); }
	            		String filenameToRead;
	            		System.out.println("Enter the name of the file that you wish to read: ");
	            		filenameToRead = scan.nextLine();
	            		int chunkNumberRead = 0;
	            		//this.accessUserInput = false;
	            		sendClientReadFileRequestToController(chunkNumberRead, filenameToRead);
	            		break;
	            	case "Q":
	            		if (DEBUG) { System.out.println("User selected Quit."); }
	            		System.out.println("Quitting program. Goodbye.");
	            		System.exit(1);
	            	default:
	            		System.out.println("Command unrecognized. Please enter a valid input.");
	            }
        	//}
        }
	}
	
	private void connectToController() {
		if (DEBUG) { System.out.println("begin Client connectToController"); }
		try {
			System.out.println("Attempting to connect to Controller " + this.controllerNodeInformation.getNodeIPAddress() + " at Port Number: " + this.controllerNodeInformation.getNodePortNumber());
			Socket controllerSocket = new Socket(this.controllerNodeInformation.getNodeIPAddress(), this.controllerNodeInformation.getNodePortNumber());
			
			System.out.println("Starting TCPReceiverThread with Controller");
			clientTCPReceiverThread = new TCPReceiverThread(controllerSocket, this);
			Thread tcpReceiverThread = new Thread(this.clientTCPReceiverThread);
			tcpReceiverThread.start();
			
			System.out.println("TCPReceiverThread with Controller started");
			System.out.println("Sending to " + this.controllerNodeInformation.getNodeIPAddress() + " on Port " +  this.controllerNodeInformation.getNodePortNumber());
			
			this.controllerSender = new TCPSender(controllerSocket);
			
			ClientRegisterRequestToController clientRegisterRequest = new ClientRegisterRequestToController(this.clientNodeInformation.getNodeIPAddress(), this.clientNodeInformation.getNodePortNumber());

			if (DEBUG) { System.out.println("ChunkServer about to send message type: " + clientRegisterRequest.getType()); }
			
			this.controllerSender.sendData(clientRegisterRequest.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end Client connectToController"); }
	}
	
	private void handleControllerRegisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin Client handleControllerRegisterResponse"); }
		ControllerRegisterResponseToClient clientRegisterResponse = (ControllerRegisterResponseToClient) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + clientRegisterResponse.getType()); }
		
		// successful registration
		if (clientRegisterResponse.getStatusCode() == (byte) 1) {
			System.out.println("Registration Request Succeeded.");
			System.out.println(String.format("Message: %s", clientRegisterResponse.getAdditionalInfo()));
		// unsuccessful registration
		} else {
			System.out.println("Registration Request Failed. Exiting.");
            System.out.println(String.format("Message: %s", clientRegisterResponse.getAdditionalInfo()));
            System.exit(0);
		}
		if (DEBUG) { System.out.println("end Client handleControllerRegisterResponse"); }
	}
	
	private void sendClientChunkServerRequestToController(int chunkNumber, String filename, int totalNumberOfChunks) {
		if (DEBUG) { System.out.println("begin Client sendClientChunkServerRequestToController"); }
		
		try {
			ClientChunkServerRequestToController chunkServersRequest = new ClientChunkServerRequestToController(chunkNumber, this.clientNodeInformation, totalNumberOfChunks, filename);
			this.controllerSender.sendData(chunkServersRequest.getBytes());

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end Client sendClientChunkServerRequestToController"); }
	}
	
	private void handleControllerChunkServersResponse(Event event) {
		if (DEBUG) { System.out.println("begin Client handleControllerChunkServersResponse"); }
		ControllerChunkServersResponseToClient clientChunkServersFromController = (ControllerChunkServersResponseToClient) event;
		if (DEBUG) { System.out.println("Client got a message type: " + clientChunkServersFromController.getType()); }
		
		// int shardNumberWithChunkServerSize, HashMap<Integer, NodeInformation> shardNumberWithChunkServer, int chunkNumber, int totalNumberOfChunks, String filename
		String filename = clientChunkServersFromController.getFilename();
		int totalNumberOfChunks = clientChunkServersFromController.getTotalNumberOfChunks();
		int chunkNumber = clientChunkServersFromController.getChunkNumber();
		HashMap<Integer, NodeInformation> shardNumberWithChunkServer = clientChunkServersFromController.getShardNumberWithChunkServer();

		// fileWitrhChunkNumberWithShardWithChunkServers
		synchronized (fileWithChunkNumberWithShardWithChunkServers) {
			if (!fileWithChunkNumberWithShardWithChunkServers.containsKey(filename)) {
				HashMap<Integer, HashMap<Integer, NodeInformation>> tempMap = new HashMap<Integer, HashMap<Integer, NodeInformation>>();
				tempMap.put(chunkNumber, shardNumberWithChunkServer);
				fileWithChunkNumberWithShardWithChunkServers.put(filename, tempMap);
			} else {
				HashMap<Integer, HashMap<Integer, NodeInformation>> tempMap = new HashMap<Integer, HashMap<Integer, NodeInformation>>();
				tempMap = fileWithChunkNumberWithShardWithChunkServers.get(filename);
				tempMap.put(chunkNumber, shardNumberWithChunkServer);
				fileWithChunkNumberWithShardWithChunkServers.put(filename, tempMap);
			}
		}
		
		if (chunkNumber == totalNumberOfChunks) {
			encodeAndProcessStoredChunks(filename);
		} else {
			try {
				chunkNumber++;
				ClientChunkServerRequestToController chunkServersRequest = new ClientChunkServerRequestToController(chunkNumber, this.clientNodeInformation, totalNumberOfChunks, filename);
				this.controllerSender.sendData(chunkServersRequest.getBytes());
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		if (DEBUG) { System.out.println("end Client handleControllerChunkServersResponse"); }
	}
	
	private void encodeAndProcessStoredChunks(String filename) {
		synchronized (fileWithChunkNumberWithShardWithChunkServers) {
			HashMap<Integer, HashMap<Integer, NodeInformation>> chunkWithShardAndNodeInfoMap = new HashMap<Integer, HashMap<Integer, NodeInformation>>();
			chunkWithShardAndNodeInfoMap = fileWithChunkNumberWithShardWithChunkServers.get(filename);
			
			int chunkCount = 0;
			
			if (!this.fileIntoChunks.isEmpty()) {
				for (byte[] fileBytes : this.fileIntoChunks) {
					// Reed-Solomon encoding
					
					// file size
					//int fileSize = (int) inputFile.length();
					
					// total size of the stored data = length of the payload paylod size
					int storedSize = fileBytes.length + BYTES_IN_INT;
					
					// size of a shard. Make sure all the shards are of the same size.
					// In order to do this, you can padd 0s at the end.
					// This particular code works for 4 data shards.
					// Based on the numer of shards, use a appropriate way to
					// decide on shard size.
					int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;
					
					// Create a buffer holding the file size, followed by the contents of the file
					// (and padding if required)
					int bufferSize = shardSize * DATA_SHARDS;
					byte [] allBytes = new byte[bufferSize];
					
					// copying the file size, payload and padding into the byte array
					ByteBuffer.wrap(allBytes).putInt(fileBytes.length);
		            System.arraycopy(fileBytes, 0, allBytes, BYTES_IN_INT, fileBytes.length);

		            int paddingLength = bufferSize - (storedSize);
		            byte[] paddedZeros = new byte[paddingLength];
		            
		            for (int j = 0; j < paddingLength; j++) {
		                paddedZeros[j] = 0;
		            }
		            
		            if (paddingLength != 0) {
		                System.arraycopy(paddedZeros, 0, allBytes, storedSize, paddingLength);
		            }

					// Make the buffers to hold the shards.
					byte [][] shards = new byte[TOTAL_SHARDS][shardSize];
					
					// Fill in the data shards
					for (int i = 0; i < DATA_SHARDS; i++) {
						System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
					}
					
					// Use Reed-Solomon to calculate the parity. Parity codes
					// will be stored in the last two positions in 'shards' 2-D array.
					ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
					reedSolomon.encodeParity(shards, 0, shardSize);
					
					// finally store the contents of the 'shards' 2-D array
					// locallyStoredShardsWithBytes
					for (int i = 0; i < TOTAL_SHARDS; i++) {
		                synchronized (this.locallyStoredShardsWithBytes) {
		                    this.locallyStoredShardsWithBytes.putIfAbsent(i, shards[i]);
		                }
		            }
					
					if (chunkWithShardAndNodeInfoMap.containsKey(chunkCount)) {
						HashMap<Integer, NodeInformation> shardsWithChunkServerMap = new HashMap<Integer, NodeInformation>();
						shardsWithChunkServerMap = chunkWithShardAndNodeInfoMap.get(chunkCount);

						for (int shardNumber : shardsWithChunkServerMap.keySet()) {
							try {
								NodeInformation chunkServer = shardsWithChunkServerMap.get(shardNumber);
								ClientSendChunkToChunkServer chunkToChunkServerSender = new ClientSendChunkToChunkServer(shards[shardNumber - 1], chunkCount, filename, shardNumber);
								Socket chunkServerSocket = new Socket(chunkServer.getNodeIPAddress(), chunkServer.getNodePortNumber());
								
								TCPSender chunkSender = new TCPSender(chunkServerSocket);
								
								chunkSender.sendData(chunkToChunkServerSender.getBytes());
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				chunkCount++;
				}
			}
			fileWithChunkNumberWithShardWithChunkServers.clear();
		}
	}
	
	private void sendClientReadFileRequestToController(int chunkNumber, String filename) {
		if (DEBUG) { System.out.println("begin Client sendClientReadFileRequestToController"); }
		
		try {
			ClientReadFileRequestToController readRequest = new ClientReadFileRequestToController(chunkNumber, this.clientNodeInformation, filename);
			
			System.out.println("Telling the Controller to send the file to: " + this.clientNodeInformation.getNodeIPAddress());
			
			this.controllerSender.sendData(readRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end Client sendClientReadFileRequestToController"); }
		
	}
	
	private void handleControllerChunkServerToReadResponseToClient(Event event) {
		if (DEBUG) { System.out.println("begin Client handleControllerChunkServerToReadResponseToClient"); }
		ControllerChunkServerToReadResponseToClient chunkServerToReadResponse = (ControllerChunkServerToReadResponseToClient) event;
		
		// ControllerChunkServerToReadResponseToClient(int shardNumberWithChunkServerSize, HashMap<Integer, NodeInformation> shardNumberWithChunkServer, int chunkNumber, int totalNumberOfChunks, String filename
		HashMap<Integer, NodeInformation> shardNumberWithChunkServer = new HashMap<Integer, NodeInformation>();
		shardNumberWithChunkServer = chunkServerToReadResponse.getShardNumberWithChunkServer();
		int chunkNumber = chunkServerToReadResponse.getChunkNumber();
		int totalNumberOfChunks = chunkServerToReadResponse.getTotalNumberOfChunks();
		String filename = chunkServerToReadResponse.getFilename();
		
		// tell each chunk server returned that we need x chunk number and y shard with this filename
		for (int shardNumber : shardNumberWithChunkServer.keySet()) {
			NodeInformation chunkServer = shardNumberWithChunkServer.get(shardNumber);
			// NodeInformation nodeInformation, int chunkNumber, String fileName, int totalNumberOfChunks, int shardNumber
			ClientRequestToReadFromChunkServer readFromChunkServer = new ClientRequestToReadFromChunkServer(this.clientNodeInformation, chunkNumber, filename, totalNumberOfChunks, shardNumber);
			try {
				Socket chunkServerSocket = new Socket(chunkServer.getNodeIPAddress(), chunkServer.getNodePortNumber());
				
				TCPSender chunkSender = new TCPSender(chunkServerSocket);
				
				chunkSender.sendData(readFromChunkServer.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end Client handleControllerChunkServerToReadResponseToClient"); }
	}
	
	private void ChunkServerSendChunkToClient(Event event) {
		if (DEBUG) { System.out.println("begin Client ChunkServerSendChunkToClient"); }
		
		ChunkServerSendChunkToClient chunksReceived = (ChunkServerSendChunkToClient) event;
		
		int chunkNumber = chunksReceived.getChunkNumber();
		byte[] chunkData = chunksReceived.getChunkBytes();
		String filename = chunksReceived.getFilename();
		int shardNumber = chunksReceived.getShardNumber();
		int totalNumberOfChunks = chunksReceived.getTotalNumberOfChunks();

		HashMap<Integer, byte[]> shardWithBytes = new HashMap<Integer, byte[]>();

		System.out.println("Storing Filename: " + filename + " with chunk number: " + chunkNumber + " from shard number " + shardNumber);
		
		// HashMap<Integer, HashMap<Integer, byte[]>> receivedFileWithChunkNumberWithShardWithBytes;
		synchronized (receivedFileWithChunkNumberWithShardWithBytes) {
			if (!this.receivedFileWithChunkNumberWithShardWithBytes.containsKey(chunkNumber)) {
				shardWithBytes.put(shardNumber, chunkData);
				this.receivedFileWithChunkNumberWithShardWithBytes.put(chunkNumber, shardWithBytes);
			} else {
				HashMap<Integer, byte[]> tempChunkWithBytes = this.receivedFileWithChunkNumberWithShardWithBytes.get(chunkNumber);
				tempChunkWithBytes.put(shardNumber, chunkData);
				this.receivedFileWithChunkNumberWithShardWithBytes.put(chunkNumber, tempChunkWithBytes);
			}
			// need to check if we have all the shards for this chunk before moving on to the next chunk
			int numberOfShards = this.receivedFileWithChunkNumberWithShardWithBytes.get(chunkNumber).size();
			
			if (numberOfShards == TOTAL_SHARDS) {
				// received all shards for this chunk, need to ensure that we have all of the chunks associated with this file before merging
				int numberOfReceivedChunks = this.receivedFileWithChunkNumberWithShardWithBytes.size();
				if (numberOfReceivedChunks == totalNumberOfChunks) {
					// all chunks have been received, can decode and merge the file
					encodeAndProcessReceivedChunks(filename);
				} else {
					// still more chunks to get, but done with the shard
					chunkNumber++;
					sendClientReadFileRequestToController(chunkNumber, filename);
				}
			} else {
				// still waiting more shards to get for this chunk
				System.out.println("Still waiting for more shards for this chunk numebr.");
			}
		}
		if (DEBUG) { System.out.println("end Client ChunkServerSendChunkToClient"); }
	}
	
	private void encodeAndProcessReceivedChunks(String filename) {
		
		String path = System.getProperty("user.dir") + "/tmp/received/";
		File pathFile = new File(path);
		if (!pathFile.exists()) {
			pathFile.mkdir();
		}
		
		String receivedFilePath = path + filename;
		
		File receivedFile = new File(receivedFilePath);
		
		try {
			if (!receivedFile.exists()) {
					receivedFile.createNewFile();
			} 
			
			FileOutputStream fos = new FileOutputStream(receivedFile);
			
			// cycle through all of the stored chunks, decode and store
			// HashMap<Integer, HashMap<Integer, byte[]>> receivedFileWithChunkNumberWithShardWithBytes
			for (HashMap.Entry<Integer, HashMap<Integer, byte[]>> entrySet : receivedFileWithChunkNumberWithShardWithBytes.entrySet()) {
				int chunkNumber = entrySet.getKey();
				HashMap<Integer, byte[]> tempChunkWithBytes = this.receivedFileWithChunkNumberWithShardWithBytes.get(chunkNumber);
				
				// Read in any of the shards that are present.
				// (There should be checking here to make sure the input
				// shards are the same size, but there isn't.)
				byte [] [] shards = new byte [TOTAL_SHARDS] [];
				boolean [] shardPresent = new boolean [TOTAL_SHARDS];
				
				int shardSize = 0;
				int shardCount = 0;
				//int shardNumber = 1;
				
				// now read the shards from the persistance store
				for (Integer shardNumber : tempChunkWithBytes.keySet()) {
					byte[] tempReceivedBytes = tempChunkWithBytes.get(shardNumber);
					
					// Check if the shard is available.
					// If available, read its content into shards[i]
					// set shardPresent[i] = true and increase the shardCount by 1.
					shards[shardNumber] = tempReceivedBytes;
                    shardPresent[shardNumber] = true;
                    shardCount++;
                    shardSize = tempReceivedBytes.length;
				}
				
				// We need at least DATA_SHARDS to be able to reconstruct the file.
				if (shardCount < DATA_SHARDS) {
					System.out.println("Not enough DATA_SHARDS received to reconstruct the file.");
					return;
				}
				
				// Make empty buffers for the missing shards.
				for (int i = 0; i < TOTAL_SHARDS; i++) {
					if (!shardPresent[i]) {
						shards[i] = new byte [shardSize];
					}
				}
				
				// Use Reed-Solomon to fill in the missing shards
				ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
				reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);
				
				byte[] allbytes = new byte[shardSize * DATA_SHARDS];
	            for (int i = 0; i < DATA_SHARDS; i++) {
	                System.arraycopy(shards[i], 0, allbytes, shardSize * i, shardSize);
	            }
				
	            int chunkSize = ByteBuffer.wrap(allbytes).getInt();
	            byte[] chunk = new byte[chunkSize];
	            System.arraycopy(allbytes, BYTES_IN_INT, chunk, 0, chunkSize);
	            fos.write(chunk);
			}
			
			fos.close();
			System.out.println("File has been saved to the following location: " + pathFile.getAbsolutePath());
			
			// remove the file from the map, no longer building it
			this.receivedFileWithChunkNumberWithShardWithBytes.clear();

			System.out.println("Finished merging file: " + filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static ArrayList<byte[]> splitFileIntoBytes(File file) {
		ArrayList<byte[]> filesAsBytesList = new ArrayList<byte[]>();
		
		byte[] chunkSizeBytes = new byte[SIZE_OF_CHUNK];
		
		
		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			int fileLength = 0;
			
			while ((fileLength = bis.read(chunkSizeBytes)) > 0) {
				chunkSizeBytes = Arrays.copyOf(chunkSizeBytes, fileLength);
				
				// add the bytes to the ArrayList that holds all of the bytes for the file
				filesAsBytesList.add(chunkSizeBytes);
				chunkSizeBytes = new byte[SIZE_OF_CHUNK];
			}
			bis.close();
		
		} catch  (IOException ioe) {
			ioe.printStackTrace();
		}
		
		return filesAsBytesList;
	}
}
