package cs555.erasure.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import cs555.erasure.transport.TCPHeartbeat;
import cs555.erasure.transport.TCPReceiverThread;
import cs555.erasure.transport.TCPSender;
import cs555.erasure.transport.TCPServerThread;
import cs555.erasure.util.Metadata;
import cs555.erasure.util.NodeInformation;
import cs555.erasure.wireformats.ChunkServerRegisterRequestToController;
import cs555.erasure.wireformats.ControllerRegisterResponseToChunkServer;
import cs555.erasure.wireformats.Event;
import cs555.erasure.wireformats.Protocol;

/**
 * Chunk Server responsible for managing file chunks. There will be one instance of the chunk
 * server running on each machine.
 *
 */

public class ChunkServer implements Node {
	
	private static boolean DEBUG = false;
	private NodeInformation controllerNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	private HashMap<String, ArrayList<Integer>> filesWithChunkNumberMap;
	private HashMap<String, Metadata> filesWithMetadataMap;
	private AtomicLong localFileSize;
	private ArrayList<Metadata> newMetadataList;
	private static final String FILE_SPACE_LOCATION = System.getProperty("user.dir");
	private static final String FILE_LOCATION = System.getProperty("user.dir") + "/tmp/data/";
	private String tempFileLocationReplacement;
	private String localFilePath;
	private TCPReceiverThread chunkServerTCPReceiverThread;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender controllerSender;
	private static ChunkServer chunkServer;
	private NodeInformation chunkServerNodeInformation;
	
	private static final int SIZE_OF_SLICE = 1024 * 8;

	public ChunkServer() {}
	
	// public methods for access in the heartbeats
	public ArrayList<Metadata> getFilesWithMetadataMap() {
		ArrayList<Metadata> metadataList = new ArrayList<Metadata>();
		synchronized (filesWithMetadataMap ) {
			for (Metadata m : this.filesWithMetadataMap.values()) {
				metadataList.add(m);
			}
		}
		return metadataList;
	}
	
	public ArrayList<Metadata> getNewFilesWithMetadataMap() {
		synchronized (newMetadataList) {
			return newMetadataList;
		}
	}
	
	public void clearNewMetadataList() {
		synchronized (newMetadataList){
			this.newMetadataList.clear();
		}
	}
	
	public long getFreeSpaceAvailable() {
		/**
		File localDirectory = new File(this.tempFileLocationReplacement);
		long directoryLength = 0;
	    for (File file : localDirectory.listFiles()) {
	        if (file.isFile())
	        	directoryLength += file.length();
	    }
		**/
		return new File(FILE_SPACE_LOCATION).getFreeSpace() - this.localFileSize.get();
	}
	
	public int getNumberOfChunksStored() {
		int totalNumberOfChunks = 0;
		synchronized (filesWithChunkNumberMap) {
			for (ArrayList<Integer> chunkList : filesWithChunkNumberMap.values()) {
				totalNumberOfChunks += chunkList.size();
			}
		}
		return totalNumberOfChunks;
	}
	
	public TCPSender getChunkServerSender() {
		return this.controllerSender;
	}
	
	private ChunkServer(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.filesWithChunkNumberMap = new HashMap<String, ArrayList<Integer>>();
		this.filesWithMetadataMap = new HashMap<String, Metadata>();
		this.newMetadataList = new ArrayList<Metadata>();
		this.localFileSize = new AtomicLong(0);
		
		try {
			TCPServerThread serverThread = new TCPServerThread(0, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			this.localFilePath = "/" + InetAddress.getLocalHost().getHostName() + "/tmp/data/";
			this.tempFileLocationReplacement = FILE_LOCATION + this.localFilePath;
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		this.chunkServerNodeInformation = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
		// Once the initialization is complete, chunkServer should send a registration request to the controller.
		connectToController();
	}
	
	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event " + eventType + " Passed to ChunkServer."); }
		switch(eventType) {
			// CONTROLLER_REGISTER_RESPONSE_TO_CHUNKSERVER = 6000
			case Protocol.CONTROLLER_REGISTER_RESPONSE_TO_CHUNKSERVER:
				handleChunkServerRegisterResponse(event);	
				break;
			// CONTROLLER_HEARTBEAT_TO_CHUNKSERVER = 6004:
			case Protocol.CONTROLLER_HEARTBEAT_TO_CHUNKSERVER:
				//if (DEBUG) { System.out.println("Heartbeat from Controller."); }
				break;
			/**
			// CONTROLLER_FORWARD_DATA_TO_NEW_CHUNKSERVER = 6005
			case Protocol.CONTROLLER_FORWARD_DATA_TO_NEW_CHUNKSERVER:
				handleControllerForwardDataToNewChunkServer(event);
				break;
			// CONTROLLER_FORWARD_CORRUPT_CHUNK_TO_CHUNKSERVER = 6006
			case Protocol.CONTROLLER_FORWARD_CORRUPT_CHUNK_TO_CHUNKSERVER:
				handleControllerForwardFixCorruptChunkToChunkServer(event);
				break;
			// CONTROLLER_FORWARD_ONLY_CORRUPT_CHUNK_TO_CHUNKSERVER = 6007
			case Protocol.CONTROLLER_FORWARD_ONLY_CORRUPT_CHUNK_TO_CHUNKSERVER:
				handleControllerForwardOnlyFixCorruptChunkToChunkServer(event);
				break;
			// CHUNKSERVER_SEND_CHUNK_TO_LAST_CHUNKSERVER = 7001
			case Protocol.CHUNKSERVER_SEND_CHUNK_TO_LAST_CHUNKSERVER:
				handleLastDataReceived(event);
				break;
			// CHUNKSERVER_FIX_CORRUPT_CHUNK_TO_CHUNKSERVER = 7006
			case Protocol.CHUNKSERVER_FIX_CORRUPT_CHUNK_TO_CHUNKSERVER:
				handleChunkServerFixCorruptChunkToChunkServer(event);
				break;
			// CLIENT_SEND_CHUNK_TO_CHUNKSERVER = 8002
			case Protocol.CLIENT_SEND_CHUNK_TO_CHUNKSERVER:
				handleChunkDataReceieved(event);
				break;
			// CLIENT_READ_REQUEST_TO_CHUNKSERVER = 8004
			case Protocol.CLIENT_READ_REQUEST_TO_CHUNKSERVER:
				handleClientRequestToReadFromChunkServer(event);
				break;
				**/
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
		
		// requires 2 arguments to initialize a chunkServer
		if(args.length != 2) {
            System.out.println("Invalid Arguments. Must include controller host name and port number.");
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
		
		chunkServer = new ChunkServer(controllerIPAddress, controllerPortNumber);
		
	}
	
	private void connectToController() {
		if (DEBUG) { System.out.println("begin ChunkServer connectToController"); }
		try {
			System.out.println("Attempting to connect to Controller " + this.controllerNodeInformation.getNodeIPAddress() + " at Port Number: " + this.controllerNodeInformation.getNodePortNumber());
			Socket controllerSocket = new Socket(this.controllerNodeInformation.getNodeIPAddress(), this.controllerNodeInformation.getNodePortNumber());
			
			System.out.println("Starting TCPReceiverThread with Controller");
			chunkServerTCPReceiverThread = new TCPReceiverThread(controllerSocket, this);
			Thread tcpReceiverThread = new Thread(this.chunkServerTCPReceiverThread);
			tcpReceiverThread.start();
			
			System.out.println("TCPReceiverThread with Controller started");
			System.out.println("Sending to " + this.controllerNodeInformation.getNodeIPAddress() + " on Port " +  this.controllerNodeInformation.getNodePortNumber());
			
			this.controllerSender = new TCPSender(controllerSocket);
			
			File file = new File(FILE_SPACE_LOCATION);
			long freeSpace = file.getFreeSpace();
			
			ChunkServerRegisterRequestToController chunkServerRegisterRequest = new ChunkServerRegisterRequestToController(this.chunkServerNodeInformation.getNodeIPAddress(), this.chunkServerNodeInformation.getNodePortNumber(), freeSpace);

			if (DEBUG) { System.out.println("ChunkServer about to send message type: " + chunkServerRegisterRequest.getType()); }
			
			this.controllerSender.sendData(chunkServerRegisterRequest.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end ChunkServer connectToController"); }
	}
	
	private void handleChunkServerRegisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleChunkServerRegisterResponse"); }
		ControllerRegisterResponseToChunkServer chunkServerRegisterResponse = (ControllerRegisterResponseToChunkServer) event;
		if (DEBUG) { System.out.println("ChunkServer got a message type: " + chunkServerRegisterResponse.getType()); }
		
		// successful registration
		if (chunkServerRegisterResponse.getStatusCode() == (byte) 1) {
			System.out.println("Registration Request Succeeded.");
			System.out.println(String.format("Message: %s", chunkServerRegisterResponse.getAdditionalInfo()));
			
			TCPHeartbeat tCPHeartbeat = new TCPHeartbeat(chunkServer, chunkServerNodeInformation);
			Thread tCPHeartBeatThread = new Thread(tCPHeartbeat);
			tCPHeartBeatThread.start();

		// unsuccessful registration
		} else {
			System.out.println("Registration Request Failed. Exiting.");
            System.out.println(String.format("Message: %s", chunkServerRegisterResponse.getAdditionalInfo()));
            System.exit(0);
		}
		if (DEBUG) { System.out.println("end ChunkServer handleChunkServerRegisterResponse"); }
	}

}
