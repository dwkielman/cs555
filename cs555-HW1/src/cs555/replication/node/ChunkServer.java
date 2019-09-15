package cs555.replication.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import cs555.replication.transport.TCPReceiverThread;
import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.Metadata;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerRegisterRequestToController;
import cs555.replication.wireformats.ChunkServerSendChunkToLastChunkServer;
import cs555.replication.wireformats.ClientSendChunkToChunkServer;
import cs555.replication.wireformats.ControllerRegisterResponseToChunkServer;
import cs555.replication.wireformats.Event;
import cs555.replication.wireformats.Protocol;

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
	// may not need this, instead may need something for storing the location of where data is and a file name. doesn't matter which client requests the data,
	// just need to send the correct file. will include the client node in the message to the chunk server
	private HashMap<NodeInformation, TCPSender> clientNodesMap;
	private HashMap<String, ArrayList<Integer>> filesWithChunkNumberMap;
	private HashMap<String, Metadata> filesWithMetadataMap;
	private final String FILE_LOCATION = "/tmp/data";
	private TCPReceiverThread chunkServerTCPReceiverThread;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender chunkServerSender;
	
	private static final int SIZE_OF_SLICE = 1024 * 8;

	private ChunkServer(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.clientNodesMap = new HashMap<NodeInformation, TCPSender>();
		this.filesWithChunkNumberMap = new HashMap<String, ArrayList<Integer>>();
		this.filesWithMetadataMap = new HashMap<String, Metadata>();
		
		try {
			TCPServerThread serverThread = new TCPServerThread(0, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		// Once the initialization is complete, MessagingNode should send a registration request to the Registry.
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
			// CHUNKSERVER_SEND_CHUNK_TO_LAST_CHUNKSERVER = 7001
			case Protocol.CHUNKSERVER_SEND_CHUNK_TO_LAST_CHUNKSERVER:
				handleLastDataReceived(event);
				break;
			// CLIENT_SEND_CHUNK_TO_CHUNKSERVER = 8002
			case Protocol.CLIENT_SEND_CHUNK_TO_CHUNKSERVER:
				handleChunkDataReceieved(event);
				break;
			// CLIENT_READ_REQUEST_TO_CHUNKSERVER = 8004
			case Protocol.CLIENT_READ_REQUEST_TO_CHUNKSERVER:
				handleClientRequestToReadFromChunkServer(event);
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
		
		ChunkServer chunkServer = new ChunkServer(controllerIPAddress, controllerPortNumber);
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
			
			this.chunkServerSender = new TCPSender(controllerSocket);
			
			File file = new File(FILE_LOCATION);
			long freeSpace = file.getFreeSpace();
			
			ChunkServerRegisterRequestToController chunkServerRegisterRequest = new ChunkServerRegisterRequestToController(this.controllerNodeInformation.getNodeIPAddress(), this.controllerNodeInformation.getNodePortNumber(), freeSpace);

			if (DEBUG) { System.out.println("ChunkServer about to send message type: " + chunkServerRegisterRequest.getType()); }
			
			this.chunkServerSender.sendData(chunkServerRegisterRequest.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end ChunkServer connectToController"); }
	}
	
	private void handleChunkServerRegisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleChunkServerRegisterResponse"); }
		ControllerRegisterResponseToChunkServer chunkServerRegisterResponse = (ControllerRegisterResponseToChunkServer) event;
		if (DEBUG) { System.out.println("MessagingNode got a message type: " + chunkServerRegisterResponse.getType()); }
		
		// successful registration
		if (chunkServerRegisterResponse.getStatusCode() == (byte) 1) {
			System.out.println("Registration Request Succeeded.");
			System.out.println(String.format("Message: %s", chunkServerRegisterResponse.getAdditionalInfo()));
		// unsuccessful registration
		} else {
			System.out.println("Registration Request Failed. Exiting.");
            System.out.println(String.format("Message: %s", chunkServerRegisterResponse.getAdditionalInfo()));
            System.exit(0);
		}
		if (DEBUG) { System.out.println("end ChunkServer handleChunkServerRegisterResponse"); }
	}
	
	private void handleChunkDataReceieved(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleChunkServerRegisterResponse"); }
		ClientSendChunkToChunkServer chunkDataReceived = (ClientSendChunkToChunkServer) event;
		
		String filename = chunkDataReceived.getFilename();
		int chunkNumber = chunkDataReceived.getChunkNumber();
		long timestamp = chunkDataReceived.getTimestamp();
		byte[] chunkData = chunkDataReceived.getChunkBytes();
		int version = 1;
		ArrayList<Integer> chunkNumbers = new ArrayList<Integer>();
		
		// file is not currently stored on the server, need to add it for the first time
		if (!filesWithChunkNumberMap.containsKey(filename)) {
			chunkNumbers.add(chunkNumber);
			filesWithChunkNumberMap.put(filename, chunkNumbers);
			saveFile(filename, chunkData, chunkNumber, version);
		} else {
		// file already exists on the server, could be a new chunk number or one that already exists
			chunkNumbers = filesWithChunkNumberMap.get(filename);
			
			// if the chunkNumber isn't already stored, then add the new chunk data to the existing filename
			if (!chunkNumbers.contains(chunkNumber)) {
				filesWithChunkNumberMap.get(filename).add(chunkNumber);
				saveFile(filename, chunkData, chunkNumber, version);
			} else {
				// make sure that the timestamp is more recent than the one that is currently stored on the server
				// make sure we have metadata for the file before trying to update it
				String metadataFilename = filename + "_chunk" + chunkNumber;
				if (filesWithMetadataMap.containsKey(metadataFilename) ) {
					Metadata metadata = filesWithMetadataMap.get(metadataFilename);
					long metadataTimestamp = metadata.getTimestamp();
					
					// metadata is older than the new file
					if (metadataTimestamp < timestamp) {
						int newVersionNumber = metadata.getVersionInfoNumber() + 1;
						saveFile(filename, chunkData, chunkNumber, newVersionNumber);
					} else {
						System.out.println("No action taken, file sent is older than the previous version.");
					}
				}
			}
		}
		
		// send the data to the other chunkServers if there are any contained here
		ArrayList<NodeInformation> chunkServers = chunkDataReceived.getChunkServersNodeInfoList();
		
		// more than 1 chunkServers requires more chunkServer info
		if (chunkServers.isEmpty()) {
			System.out.println("Error: no chunkServers left but need to still send.");
		} else {
			NodeInformation firstChunkServer = chunkServers.remove(0);
			
			try {
				Socket chunkServer = new Socket(firstChunkServer.getNodeIPAddress(), firstChunkServer.getNodePortNumber());
				
				// more chunkServers to send to so use same protocol with chunk servers
				if (!chunkServers.isEmpty()) {
					ClientSendChunkToChunkServer chunksToChunkServer = new ClientSendChunkToChunkServer(chunkServers.size(), chunkServers, chunkData, chunkNumber, filename, timestamp);
					TCPSender chunkSender = new TCPSender(chunkServer);
					chunkSender.sendData(chunksToChunkServer.getBytes());
					
				} else if (chunkServers.size() == 1) {
					// only one chunkServer left so use different protocol
					ChunkServerSendChunkToLastChunkServer chunksToLastChunkServer = new ChunkServerSendChunkToLastChunkServer(chunkData, chunkNumber, filename, timestamp);
					TCPSender chunkSender = new TCPSender(chunkServer);
					chunkSender.sendData(chunksToLastChunkServer.getBytes());
				} 
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (DEBUG) { System.out.println("end ChunkServer handleChunkDataReceieved"); }
	}
	
	private void handleLastDataReceived(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleLastDataReceived"); }
		ChunkServerSendChunkToLastChunkServer chunkDataReceived = (ChunkServerSendChunkToLastChunkServer) event;
		
		String filename = chunkDataReceived.getFilename();
		int chunkNumber = chunkDataReceived.getChunkNumber();
		long timestamp = chunkDataReceived.getTimestamp();
		byte[] chunkData = chunkDataReceived.getChunkBytes();
		int version = 1;
		ArrayList<Integer> chunkNumbers = new ArrayList<Integer>();
		
		// file is not currently stored on the server, need to add it for the first time
		if (!filesWithChunkNumberMap.containsKey(filename)) {
			chunkNumbers.add(chunkNumber);
			filesWithChunkNumberMap.put(filename, chunkNumbers);
			saveFile(filename, chunkData, chunkNumber, version);
		} else {
		// file already exists on the server, could be a new chunk number or one that already exists
			chunkNumbers = filesWithChunkNumberMap.get(filename);
			
			// if the chunkNumber isn't already stored, then add the new chunk data to the existing filename
			if (!chunkNumbers.contains(chunkNumber)) {
				filesWithChunkNumberMap.get(filename).add(chunkNumber);
				saveFile(filename, chunkData, chunkNumber, version);
			} else {
				// make sure that the timestamp is more recent than the one that is currently stored on the server
				// make sure we have metadata for the file before trying to update it
				String metadataFilename = filename + "_chunk" + chunkNumber;
				if (filesWithMetadataMap.containsKey(metadataFilename) ) {
					Metadata metadata = filesWithMetadataMap.get(metadataFilename);
					long metadataTimestamp = metadata.getTimestamp();
					
					// metadata is older than the new file
					if (metadataTimestamp < timestamp) {
						int newVersionNumber = metadata.getVersionInfoNumber() + 1;
						saveFile(filename, chunkData, chunkNumber, newVersionNumber);
					} else {
						System.out.println("No action taken, file sent is older than the previous version.");
					}
				}
			}
		}
		
		if (DEBUG) { System.out.println("end ChunkServer handleLastDataReceived"); }
	}
	
	private void handleClientRequestToReadFromChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleClientRequestToReadFromChunkServer"); }
		
		// stopping here for now, need to get the info from request sent from the client, find the data, validate that it is correct, then send it TO the client
		
		// if the data is invalid then need to send some error message/status that the data is wrong to both the client and to the controller
		
		// controller should remove this node from the valid nodes for this chunk of the data and send another chunkserver to the client to get the correct data
		
		// should the client attempt to heal the file at this point? or wait until later?
		
		
		
		if (DEBUG) { System.out.println("end ChunkServer handleClientRequestToReadFromChunkServer"); }
	}
	
	private void saveFile(String fileName, byte[] chunkData, int chunkNumber, int versionNumber) {
		//String fileAbsolutePath = FILE_LOCATION + fileName;
		String path = FILE_LOCATION + fileName + "_chunk" + chunkNumber;
		File fileLocationToBeSaved = new File(path.substring(0, path.lastIndexOf("/")));
		
		if (!fileLocationToBeSaved.exists()) {
			fileLocationToBeSaved.mkdirs();
		}
		
		File fileToBeSaved = new File(path);
		
		try {
			// save file to the local system
			FileOutputStream fos = new FileOutputStream(fileToBeSaved);
			fos.write(chunkData, 0, chunkData.length);
			
			// generate metadata to write for saving in a different file
			Metadata metadata = new Metadata(versionNumber);
			
			// encrypt the data to create the checksum
			metadata.generataSHA1Checksum(chunkData, SIZE_OF_SLICE);
			
			String metadataFileLocation = path + ".metadata";
			this.filesWithMetadataMap.put(path, metadata);
			
			File metadataFile = new File(metadataFileLocation);
			
			byte[] metadataByteToWrite = metadata.generateMetadataBytesToWrite(chunkData);
			FileOutputStream metadataFos = new FileOutputStream(metadataFile);
			
			metadataFos.write(metadataByteToWrite, 0, metadataByteToWrite.length);
			
		} catch (FileNotFoundException e) {
			System.out.println("ChunkServer: Error in saveFile: File location not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("ChunkServer: Error in saveFile: Writing file failed.");
			e.printStackTrace();
		}
	}
}
