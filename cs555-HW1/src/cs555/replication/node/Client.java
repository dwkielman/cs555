package cs555.replication.node;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

import cs555.replication.transport.TCPReceiverThread;
import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerSendChunkToClient;
import cs555.replication.wireformats.ClientChunkServerRequestToController;
import cs555.replication.wireformats.ClientReadFileRequestToController;
import cs555.replication.wireformats.ClientRegisterRequestToController;
import cs555.replication.wireformats.ClientRequestToReadFromChunkServer;
import cs555.replication.wireformats.ClientSendChunkToChunkServer;
import cs555.replication.wireformats.ControllerChunkServerToReadResponseToClient;
import cs555.replication.wireformats.ControllerChunkServersResponseToClient;
import cs555.replication.wireformats.ControllerRegisterResponseToClient;
import cs555.replication.wireformats.ControllerReleaseClient;
import cs555.replication.wireformats.Event;
import cs555.replication.wireformats.Protocol;

/**
 * Client which is responsible for storing, retrieving, and updating files in the system. The client
 * is responsible for splitting a file into chunks and assembling the file back using chunks during
 * retrieval.
 */

public class Client implements Node {
	
	private static boolean DEBUG = true;
	private NodeInformation controllerNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	private TCPReceiverThread clientTCPReceiverThread;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender controllerSender;
	private boolean accessUserInput;
	private ArrayList<byte[]> fileIntoChunks;
	private NodeInformation clientNodeInformation;
	private HashMap<String, HashMap<Integer, byte[]>> receivedChunksMap;
	
	private static final int SIZE_OF_CHUNK = 1024 * 64;
	
	private Client(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		
		try {
			TCPServerThread serverThread = new TCPServerThread(0, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			this.accessUserInput = true;
			this.fileIntoChunks = new ArrayList<byte[]>();
			this.receivedChunksMap = new HashMap<String, HashMap<Integer, byte[]>>();
			
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
			// CONTROLLER_RELEASE_CLIENT = 6008
			case Protocol.CONTROLLER_RELEASE_CLIENT:
				handleControllerReleaseClient(event);
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
	public void setLocalHostPortNumber(int portNumber) {
		this.localHostPortNumber = portNumber;
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
		
		handleUserInput(client);
	}
	
	private static void handleUserInput(Client client) {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Ready for input.");
			
        while(true) {
        	if (client.accessUserInput) {
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
							client.accessUserInput = false;
							int chunkNumber = 0;
							long timestamp = file.lastModified();
							client.fileIntoChunks = splitFileIntoBytes(file, chunkNumber);
							client.sendClientChunkServerRequestToController(filename, chunkNumber, timestamp);
						} else {
							System.out.println("File does not exist. Please try again.");
						}
	            		break;
	            	case "R":
	            		if (DEBUG) { System.out.println("User selected Read a file."); }
	            		String filenameToRead;
	            		System.out.println("Enter the name of the file that you wish to read: ");
	            		filenameToRead = scan.nextLine();
	            		client.accessUserInput = false;
	            		client.sendClientReadFileRequestToController(filenameToRead, 0);
	            		break;
	            	case "Q":
	            		if (DEBUG) { System.out.println("User selected Quit."); }
	            		System.out.println("Quitting program. Goodbye.");
	            		System.exit(1);
	            	case "T":
	            		if (DEBUG) { System.out.println("Testing features. Testing reading a file first. Enter file name:"); }
	            		
	            		String currentDirectory = System.getProperty("user.dir");
	            	    System.out.println("The current working directory is " + currentDirectory);
	            	    String currentUser = System.getProperty("user.name");
	            	    System.out.println("The current user on this system is " + currentUser);
	            	    
	            		String filename2;
	            		System.out.println("Enter the name of the file that you wish to store: ");
						filename2 = scan.nextLine();
						Path path = Paths.get(String.format("./%s", filename2));
						File file2 = path.toFile();
						if (file2.exists()) {
							System.out.println("File Found. Good job Daniel.");
						} else {
							System.out.println("File does not exist. Please try again.");
						}
	            		break;
	            	default:
	            		System.out.println("Command unrecognized. Please enter a valid input.");
	            }
        	}
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
			
			ClientRegisterRequestToController clientRegisterRequest = new ClientRegisterRequestToController(clientNodeInformation.getNodeIPAddress(), clientNodeInformation.getNodePortNumber());

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
	
	private void sendClientChunkServerRequestToController(String filename, int chunkNumber, long timestamp) {
		if (DEBUG) { System.out.println("begin Client sendClientChunkServerRequestToController"); }
		
		try {
			ClientChunkServerRequestToController chunkServersRequest = new ClientChunkServerRequestToController(clientNodeInformation, chunkNumber, filename, timestamp);
			this.controllerSender.sendData(chunkServersRequest.getBytes());

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end Client sendClientChunkServerRequestToController"); }
	}
	
	private void sendClientReadFileRequestToController(String filename, int chunkNumber) {
		if (DEBUG) { System.out.println("begin Client sendClientReadFileRequestToController"); }
		
		try {
			ClientReadFileRequestToController readRequest = new ClientReadFileRequestToController(clientNodeInformation, filename, chunkNumber);
			this.controllerSender.sendData(readRequest.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end Client sendClientReadFileRequestToController"); }
		
	}
	
	private void handleControllerChunkServersResponse(Event event) {
		if (DEBUG) { System.out.println("begin Client handleControllerChunkServersResponse"); }
		ControllerChunkServersResponseToClient clientChunkServersFromController = (ControllerChunkServersResponseToClient) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + clientChunkServersFromController.getType()); }
		
		ArrayList<NodeInformation> chunkServersNodeInfoList = clientChunkServersFromController.getChunkServersNodeInfoList();
		int chunkNumber = clientChunkServersFromController.getChunkNumber();
		String filename = clientChunkServersFromController.getFilename();
		long timestamp = clientChunkServersFromController.getTimestamp();
		
		if (!chunkServersNodeInfoList.isEmpty()) {
			NodeInformation firstChunkServer = chunkServersNodeInfoList.remove(0);
			
			if (!fileIntoChunks.isEmpty()) {
				byte[] chunksToSend = fileIntoChunks.get(chunkNumber);
				try {
					Socket chunkServer = new Socket(firstChunkServer.getNodeIPAddress(), firstChunkServer.getNodePortNumber());

					ClientSendChunkToChunkServer chunksToChunkServer = new ClientSendChunkToChunkServer(chunkServersNodeInfoList.size(), chunkServersNodeInfoList, chunksToSend, chunkNumber, filename, timestamp);
					TCPSender chunkSender = new TCPSender(chunkServer);
					chunkSender.sendData(chunksToChunkServer.getBytes());
					
					// last chunk was just sent, clear everything and reset for input
					if (chunkNumber == fileIntoChunks.size() - 1) {
						fileIntoChunks = new ArrayList<byte[]>();
						this.accessUserInput = true;
						System.out.println("All done dividing and sending chunks.");
					// not the last chunk, need to prep the next chunk and request more chunk servers from the controller
					} else {
						chunkNumber++;
						sendClientChunkServerRequestToController(filename, chunkNumber, timestamp);
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				System.out.println("Data to pull from list is empty.");
			}
		} else {
			System.out.println("No Chunk Servers available.");
		}
		if (DEBUG) { System.out.println("end Client handleControllerChunkServersResponse"); }
	}
	
	private void handleControllerChunkServerToReadResponseToClient(Event event) {
		if (DEBUG) { System.out.println("begin Client handleControllerChunkServerToReadResponseToClient"); }
		ControllerChunkServerToReadResponseToClient chunkServerToReadResponse = (ControllerChunkServerToReadResponseToClient) event;
		
		int chunkNumber = chunkServerToReadResponse.getChunkNumber();
		int totalNumberOfChunks = chunkServerToReadResponse.getTotalNumberOfChunks();
		NodeInformation chunkServerNodeInformation = chunkServerToReadResponse.getChunkServerNodeInformation();
		String filename = chunkServerToReadResponse.getFilename();
		
		try {
			// send a request to the chunkserver for the chunk that we have gotten
			Socket chunkServer = new Socket(chunkServerNodeInformation.getNodeIPAddress(), chunkServerNodeInformation.getNodePortNumber());
			
			ClientRequestToReadFromChunkServer requestToReadFromChunkServer = new ClientRequestToReadFromChunkServer(clientNodeInformation, chunkNumber, filename, totalNumberOfChunks);
			
			TCPSender chunkSender = new TCPSender(chunkServer);
			chunkSender.sendData(requestToReadFromChunkServer.getBytes());
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		// receivedChunksMap
		if (DEBUG) { System.out.println("end Client handleControllerChunkServerToReadResponseToClient"); }
	}
	
	private void ChunkServerSendChunkToClient(Event event) {
		if (DEBUG) { System.out.println("begin Client ChunkServerSendChunkToClient"); }
		
		ChunkServerSendChunkToClient chunksReceived = (ChunkServerSendChunkToClient) event;
		
		int chunkNumber = chunksReceived.getChunkNumber();
		byte[] chunkData = chunksReceived.getChunkBytes();
		String filename = chunksReceived.getFilename();
		int totalNumberOfChunks = chunksReceived.getTotalNumberOfChunks();

		HashMap<Integer, byte[]> chunkWithBytes = new HashMap<Integer, byte[]>();
		chunkWithBytes.put(chunkNumber, chunkData);
		
		synchronized (this.receivedChunksMap) {
			this.receivedChunksMap.put(filename, chunkWithBytes);
			if (this.receivedChunksMap.get(filename).size() == totalNumberOfChunks) {
				// file is complete, put together and build
				mergeFile(filename);
			} else {
				// file is not complete, need to request the next chunk
				chunkNumber++;
				sendClientReadFileRequestToController(filename, chunkNumber);
				
			}
		}
		if (DEBUG) { System.out.println("end Client ChunkServerSendChunkToClient"); }
	}
	
	private void handleControllerReleaseClient(Event event) {
		if (DEBUG) { System.out.println("begin Client handleControllerReleaseClient"); }
		
		ControllerReleaseClient release = (ControllerReleaseClient) event;
		this.accessUserInput = release.getAccess(); 
		if (DEBUG) { System.out.println("Access User Input is: " + this.accessUserInput); }
		
		System.out.println();
		
		if (DEBUG) { System.out.println("end Client handleControllerReleaseClient"); }
	}
	
	private static ArrayList<byte[]> splitFileIntoBytes(File file, int chunkNumber) {
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
		
		} catch  (IOException ioe) {
			ioe.printStackTrace();
		}
		
		return filesAsBytesList;
	}
	
	private void mergeFile(String filename) {
		String path = "/tmp/" + System.getProperty("user.dir") + "/received/";
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
			
			int totalNumberOfChunks = this.receivedChunksMap.get(filename).size();
			
			HashMap<Integer, byte[]> dataToWrite = this.receivedChunksMap.get(filename);
			
			for (int i=0; i < totalNumberOfChunks; i++) {
				byte[] data = dataToWrite.get(i);
				fos.write(data);
			}
			
			System.out.println("File has been saved to the following location: " + pathFile.getAbsolutePath());
			
			// remove the file from the map, no longer building it
			this.receivedChunksMap.remove(filename);
			this.accessUserInput = true;
			System.out.println("Finished merging file: " + filename);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
		
}
