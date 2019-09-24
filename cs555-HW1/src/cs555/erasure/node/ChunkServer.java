package cs555.erasure.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import cs555.erasure.transport.TCPHeartbeat;
import cs555.erasure.transport.TCPReceiverThread;
import cs555.erasure.transport.TCPSender;
import cs555.erasure.transport.TCPServerThread;
import cs555.erasure.util.Metadata;
import cs555.erasure.util.NodeInformation;
import cs555.erasure.wireformats.ChunkServerRegisterRequestToController;
import cs555.erasure.wireformats.ChunkServerSendChunkToClient;
import cs555.erasure.wireformats.ClientRequestToReadFromChunkServer;
import cs555.erasure.wireformats.ClientSendChunkToChunkServer;
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
	private HashMap<String, HashMap<Integer, ArrayList<Integer>>> filesWithChunkNumberWithShardNumber;
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
		synchronized (filesWithChunkNumberWithShardNumber) {
			// (HashMap.Entry<Integer, HashMap<Integer, byte[]>> entrySet : receivedFileWithChunkNumberWithShardWithBytes.entrySet()) {
			for (HashMap.Entry<String, HashMap<Integer, ArrayList<Integer>>> entrySet : filesWithChunkNumberWithShardNumber.entrySet()) {
				totalNumberOfChunks += entrySet.getValue().size();
			}
		}
		return totalNumberOfChunks;
	}
	
	public int getNumberOfShardsStored() {
		int totalNumberOfShards = 0;
		synchronized (filesWithChunkNumberWithShardNumber) {
			// (HashMap.Entry<Integer, HashMap<Integer, byte[]>> entrySet : receivedFileWithChunkNumberWithShardWithBytes.entrySet()) {
			for (HashMap<Integer, ArrayList<Integer>> entrySet : filesWithChunkNumberWithShardNumber.values()) {
				totalNumberOfShards += entrySet.values().size();
			}
		}
		return totalNumberOfShards;
	}
	
	public TCPSender getChunkServerSender() {
		return this.controllerSender;
	}
	
	private ChunkServer(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.filesWithChunkNumberWithShardNumber = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
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
			this.localFilePath = InetAddress.getLocalHost().getHostName() + "/tmp/data/";
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
				**/
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
	
	// ClientSendChunkToChunkServer(byte[] chunkBytes, int chunkNumber, String filename, int shardNumber)
	private void handleChunkDataReceieved(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleChunkServerRegisterResponse"); }
		ClientSendChunkToChunkServer chunkDataReceived = (ClientSendChunkToChunkServer) event;
		
		String filename = chunkDataReceived.getFilename();
		int chunkNumber = chunkDataReceived.getChunkNumber();
		byte[] chunkData = chunkDataReceived.getChunkBytes();
		int shardNumber = chunkDataReceived.getShardNumber();
		int version = 1;
		HashMap<Integer, ArrayList<Integer>> chunksWithShardsMap = new HashMap<Integer, ArrayList<Integer>>();
		ArrayList<Integer> shardNumbers = new ArrayList<Integer>();
		
		// filesWithChunkNumberWithShardNumber = new HashMap<String, HashMap<Integer, ArrayList<Integer>>>();
		// file is not currently stored on the server, need to add it for the first time
		synchronized (filesWithChunkNumberWithShardNumber) {
			if (!filesWithChunkNumberWithShardNumber.containsKey(filename)) {
				shardNumbers.add(shardNumber);
				chunksWithShardsMap.put(chunkNumber, shardNumbers);
				filesWithChunkNumberWithShardNumber.put(filename, chunksWithShardsMap);
				saveFile(filename, chunkData, chunkNumber, version, shardNumber);
			} else {
			// file already exists on the server, could be a new chunk number or one that already exists
				chunksWithShardsMap = filesWithChunkNumberWithShardNumber.get(filename);
				
				// if the chunkNumber isn't already stored, then add the new chunk data to the existing filename
				if (!chunksWithShardsMap.containsKey(chunkNumber)) {
					shardNumbers.add(shardNumber);
					chunksWithShardsMap.put(chunkNumber, shardNumbers);
					filesWithChunkNumberWithShardNumber.get(filename).putAll(chunksWithShardsMap);
					saveFile(filename, chunkData, chunkNumber, version, shardNumber);
				} else {
					// make sure we have metadata for the file before trying to update it
					shardNumbers = chunksWithShardsMap.get(chunkNumber);
					shardNumbers.add(shardNumber);
					chunksWithShardsMap.put(chunkNumber, shardNumbers);
					filesWithChunkNumberWithShardNumber.get(filename).putAll(chunksWithShardsMap);
					
					String metadataFilename = filename + "_chunk" + chunkNumber + "_shard" + shardNumber;
					synchronized (filesWithMetadataMap) {
						if (filesWithMetadataMap.containsKey(metadataFilename) ) {
							Metadata metadata = filesWithMetadataMap.get(metadataFilename);

							int newVersionNumber = metadata.getVersionInfoNumber() + 1;
							saveFile(filename, chunkData, chunkNumber, newVersionNumber, shardNumber);
						} else {
							saveFile(filename, chunkData, chunkNumber, version, shardNumber);
						}
					}
				}
			}
		}
		if (DEBUG) { System.out.println("end ChunkServer handleChunkDataReceieved"); }
	}
	
	// ClientRequestToReadFromChunkServer(NodeInformation nodeInformation, int chunkNumber, String fileName, int totalNumberOfChunks, int shardNumber)
	private void handleClientRequestToReadFromChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleClientRequestToReadFromChunkServer"); }
		ClientRequestToReadFromChunkServer clientRequest = (ClientRequestToReadFromChunkServer) event;
		
		String filename = clientRequest.getFilename();
		int chunknumber = clientRequest.getChunkNumber();
		int totalNumberOfChunks = clientRequest.getTotalNumberOfChunks();
		NodeInformation client = clientRequest.getClientNodeInformation();
		int shardNumber = clientRequest.getShardNumber();
		
		String filelocation = this.tempFileLocationReplacement + filename + "_chunk" + chunknumber  + "_shard" + shardNumber;
		
		File fileToReturn = new File(filelocation);
		
		System.out.println("Request to get file: " + filelocation);
		
		Boolean isCorrupt = true;
		
		if (fileToReturn.exists()) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				Metadata tempMetadata = new Metadata(1, chunknumber, shardNumber);

				Metadata storedMetadata = filesWithMetadataMap.get(filelocation);
				String storedChecksum = storedMetadata.getChecksum();
				
				tempMetadata.generataSHA1Checksum(tempData);
				
				raf.close();
				
				if (!tempMetadata.getChecksum().equals(storedChecksum)) {
					System.out.println("Data has been corrupted, Marking Shard as corrupted.");
					isCorrupt = false;
				}
				Socket clientServer = new Socket(client.getNodeIPAddress(), client.getNodePortNumber());
				
				TCPSender clientSender = new TCPSender(clientServer);
				// ChunkServerSendChunkToClient(byte[] chunkBytes, int chunkNumber, String filename,  int totalNumberOfChunks, int shardNumber
				ChunkServerSendChunkToClient chunkToSend = new ChunkServerSendChunkToClient(tempData, chunknumber, filename, totalNumberOfChunks, shardNumber, isCorrupt);
				
				clientSender.sendData(chunkToSend.getBytes());
				/**
				if (tempMetadata.getChecksum().equals(storedChecksum)) {
					// success, requested data is same as the one stored on this system

					Socket clientServer = new Socket(client.getNodeIPAddress(), client.getNodePortNumber());
					
					TCPSender clientSender = new TCPSender(clientServer);
					// ChunkServerSendChunkToClient(byte[] chunkBytes, int chunkNumber, String filename,  int totalNumberOfChunks, int shardNumber
					ChunkServerSendChunkToClient chunkToSend = new ChunkServerSendChunkToClient(tempData, chunknumber, filename, totalNumberOfChunks, shardNumber, isCorrupt);
					
					clientSender.sendData(chunkToSend.getBytes());
					
				} else {
					// data has been messed with in some way
					System.out.println("Data has been corrupted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
				}
				**/
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// chunk has been deleted, need to report it to the controller and get missing chunk
			System.out.println("Data has been deleted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
			
			System.out.println("Metadata files stored in memory: " + filesWithMetadataMap.toString());
			System.out.println("Filename requested: " + filename);
			System.out.println("Chunk number requested: " + chunknumber);
			System.out.println("Shard number requested: " + shardNumber);
			//synchronized (filesWithChunkNumberWithShardNumber) {
				//filesWithChunkNumberWithShardNumber.get(filename).get(chunknumber).remove(shardNumber);
			//}
		}

		if (DEBUG) { System.out.println("end ChunkServer handleClientRequestToReadFromChunkServer"); }
	}
	
	private void saveFile(String fileName, byte[] chunkData, int chunkNumber, int versionNumber, int shardNumber) {
		//String fileAbsolutePath = FILE_LOCATION + fileName;
		String path = this.tempFileLocationReplacement + fileName + "_chunk" + chunkNumber + "_shard" + shardNumber;
		File fileLocationToBeSaved = new File(path.substring(0, path.lastIndexOf("/")));
		
		if (!fileLocationToBeSaved.exists()) {
			fileLocationToBeSaved.mkdirs();
		}
		
		File fileToBeSaved = new File(path);
		
		try {
			// save file to the local system
			FileOutputStream fos = new FileOutputStream(fileToBeSaved);
			fos.write(chunkData, 0, chunkData.length);
			
			this.localFileSize.getAndAdd(fileToBeSaved.length());
			
			System.out.println("Saving file to the following location: " + fileToBeSaved.getAbsolutePath());
			
			fos.close();
			// generate metadata to write for saving in a different file
			Metadata metadata = new Metadata(versionNumber, chunkNumber, shardNumber);
			
			// encrypt the data to create the checksum
			metadata.generataSHA1Checksum(chunkData);
			
			String metadataFileLocation = path + ".metadata";
			synchronized (filesWithMetadataMap) {
				this.filesWithMetadataMap.put(path, metadata);
			}
			synchronized (newMetadataList) {
				this.newMetadataList.add(metadata);
			}
			
			File metadataFile = new File(metadataFileLocation);
			
			byte[] metadataByteToWrite = metadata.generateMetadataBytesToWrite(chunkData);
			FileOutputStream metadataFos = new FileOutputStream(metadataFile);
			
			this.localFileSize.getAndAdd(metadataFile.length());
			
			System.out.println("Saving metadata to the following location: " + metadataFile.getAbsolutePath());
			
			metadataFos.write(metadataByteToWrite, 0, metadataByteToWrite.length);
			metadataFos.close();
		} catch (FileNotFoundException e) {
			System.out.println("ChunkServer: Error in saveFile: File location not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("ChunkServer: Error in saveFile: Writing file failed.");
			e.printStackTrace();
		}
	}
}