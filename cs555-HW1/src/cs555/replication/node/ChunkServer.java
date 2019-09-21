package cs555.replication.node;

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

import cs555.replication.transport.TCPHeartbeat;
import cs555.replication.transport.TCPReceiverThread;
import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.Metadata;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerDeletedChunkToController;
import cs555.replication.wireformats.ChunkServerFixCorruptChunkToChunkServer;
import cs555.replication.wireformats.ChunkServerNotifyFixSuccessToController;
import cs555.replication.wireformats.ChunkServerRegisterRequestToController;
import cs555.replication.wireformats.ChunkServerSendChunkToClient;
import cs555.replication.wireformats.ChunkServerSendChunkToLastChunkServer;
import cs555.replication.wireformats.ChunkServerSendCorruptChunkToController;
import cs555.replication.wireformats.ChunkServerSendOnlyCorruptChunkToController;
import cs555.replication.wireformats.ClientRequestToReadFromChunkServer;
import cs555.replication.wireformats.ClientSendChunkToChunkServer;
import cs555.replication.wireformats.ControllerForwardDataToNewChunkServer;
import cs555.replication.wireformats.ControllerForwardFixCorruptChunkToChunkServer;
import cs555.replication.wireformats.ControllerForwardOnlyFixCorruptChunkToChunkServer;
import cs555.replication.wireformats.ControllerRegisterResponseToChunkServer;
import cs555.replication.wireformats.Event;
import cs555.replication.wireformats.Protocol;

/**
 * Chunk Server responsible for managing file chunks. There will be one instance of the chunk
 * server running on each machine.
 *
 */

public class ChunkServer implements Node {
	
	private static boolean DEBUG = true;
	private NodeInformation controllerNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	// may not need this, instead may need something for storing the location of where data is and a file name. doesn't matter which client requests the data,
	// just need to send the correct file. will include the client node in the message to the chunk server
	private HashMap<String, ArrayList<Integer>> filesWithChunkNumberMap;
	private HashMap<String, Metadata> filesWithMetadataMap;
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
		return new File(FILE_SPACE_LOCATION).getFreeSpace();
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
		synchronized (filesWithChunkNumberMap) {
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
		
		synchronized (filesWithChunkNumberMap) {
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
					synchronized (filesWithMetadataMap) {
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
			}
		}
		
		if (DEBUG) { System.out.println("end ChunkServer handleLastDataReceived"); }
	}
	
	private void handleClientRequestToReadFromChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleClientRequestToReadFromChunkServer"); }
		ClientRequestToReadFromChunkServer clientRequest = (ClientRequestToReadFromChunkServer) event;
		
		String filename = clientRequest.getFilename();
		int chunknumber = clientRequest.getChunkNumber();
		int totalNumberOfChunks = clientRequest.getTotalNumberOfChunks();
		
		String filelocation = this.tempFileLocationReplacement + filename + "_chunk" + chunknumber;
		
		File fileToReturn = new File(filelocation);
		
		if (fileToReturn.exists()) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				Metadata tempMetadata = new Metadata(1, chunknumber);
				
				Metadata storedMetadata = filesWithMetadataMap.get(filelocation);
				String storedChecksum = storedMetadata.getChecksum();
				
				tempMetadata.generataSHA1Checksum(tempData, SIZE_OF_SLICE);
				
				if (tempMetadata.getChecksum().equals(storedChecksum)) {
					// success, requested data is same as the one stored on this system
					NodeInformation client = clientRequest.getClientNodeInformation();
					
					Socket clientServer = new Socket(client.getNodeIPAddress(), client.getNodePortNumber());
					
					TCPSender clientSender = new TCPSender(clientServer);
					
					ChunkServerSendChunkToClient chunkToSend = new ChunkServerSendChunkToClient(tempData, chunknumber, filename, totalNumberOfChunks);
					
					clientSender.sendData(chunkToSend.getBytes());
					
				} else {
					// data has been messed with in some way
					System.out.println("Data has been corrupted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
					
					String[] storedChecksumEntries = storedChecksum.split("\n");
					String[] tempChecksumEntries = tempMetadata.getChecksum().split("\n");
					
					int storedChecksumEntriesLength = storedChecksumEntries.length;
					int tempChecksumEntriesLength = tempChecksumEntries.length;
					
					System.out.println("Stored Checksum Length: " + storedChecksumEntriesLength);
					System.out.println("Temp Checksum Length: " + tempChecksumEntriesLength);
					
					ArrayList<Integer> badSlices = new ArrayList<Integer>();
					
					// local metadata had a slice added to it or possibly modified one line
					if (storedChecksumEntriesLength >= tempChecksumEntriesLength) {
						int i;
						// loop through the entries that are there to find corrupt slices
						for (i = 0; i < tempChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
							}
						}
						// add all extra slices that were added as needing to be fixed
						for (int j = i; j < storedChecksumEntriesLength; j++) {
							badSlices.add(j);
						}
						
					} else {
						// local metadata had a slice deleted from it, need to find which slice it is
						for (int i = 0; i < storedChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
								// once the bad slice has been found, all subsequent lines will be corrupt also and need to simply be replaced
								for (int j = i; j < tempChecksumEntriesLength; j++) {
									badSlices.add(j);
								}
								break;
							}
						}
					}
					NodeInformation client = clientRequest.getClientNodeInformation();
					ChunkServerSendCorruptChunkToController corruptChunkSender = new ChunkServerSendCorruptChunkToController(chunkServerNodeInformation, client, chunknumber, filename, badSlices.size(), badSlices, totalNumberOfChunks);
					this.controllerSender.sendData(corruptChunkSender.getBytes());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// chunk has been deleted, need to report it to the controller and get missing chunk
			System.out.println("Data has been deleted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
			try {
				synchronized (filesWithChunkNumberMap) {
					filesWithChunkNumberMap.get(filename).remove(chunknumber);
				}
				ChunkServerDeletedChunkToController deletedChunk = new ChunkServerDeletedChunkToController(chunkServerNodeInformation, chunknumber, filename);
				this.controllerSender.sendData(deletedChunk.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (DEBUG) { System.out.println("end ChunkServer handleClientRequestToReadFromChunkServer"); }
	}
	private void handleControllerForwardDataToNewChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleControllerForwardDataToNewChunkServer"); }
		
		ControllerForwardDataToNewChunkServer forwardData = (ControllerForwardDataToNewChunkServer) event;
		
		int chunknumber = forwardData.getChunkNumber();
		String filename = forwardData.getFilename();
		
		String filelocation = this.tempFileLocationReplacement + filename + "_chunk" + chunknumber;
		
		File fileToReturn = new File(filelocation);
		
		if (fileToReturn.exists()) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				Metadata tempMetadata = new Metadata(1, chunknumber);
				
				Metadata storedMetadata = filesWithMetadataMap.get(filelocation);
				String storedChecksum = storedMetadata.getChecksum();
				
				tempMetadata.generataSHA1Checksum(tempData, SIZE_OF_SLICE);
				
				if (tempMetadata.getChecksum().equals(storedChecksum)) {
					// success, requested data is same as the one stored on this system
					NodeInformation newChunkServer = forwardData.getChunkServer();
					
					Socket chunkServerSocket = new Socket(newChunkServer.getNodeIPAddress(), newChunkServer.getNodePortNumber());

					long timestamp = fileToReturn.lastModified();
					
					ChunkServerSendChunkToLastChunkServer chunksToLastChunkServer = new ChunkServerSendChunkToLastChunkServer(tempData, chunknumber, filename, timestamp);
					
					TCPSender chunkSender = new TCPSender(chunkServerSocket);
					chunkSender.sendData(chunksToLastChunkServer.getBytes());
					
				} else {
					// data has been messed with in some way
					System.out.println("Data has been corrupted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
					
					String[] storedChecksumEntries = storedChecksum.split("\n");
					String[] tempChecksumEntries = tempMetadata.getChecksum().split("\n");
					
					int storedChecksumEntriesLength = storedChecksumEntries.length;
					int tempChecksumEntriesLength = tempChecksumEntries.length;
					
					System.out.println("Stored Checksum Length: " + storedChecksumEntriesLength);
					System.out.println("Temp Checksum Length: " + tempChecksumEntriesLength);
					
					ArrayList<Integer> badSlices = new ArrayList<Integer>();
					
					// local metadata had a slice added to it or possibly modified one line
					if (storedChecksumEntriesLength >= tempChecksumEntriesLength) {
						int i;
						// loop through the entries that are there to find corrupt slices
						for (i = 0; i < tempChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
							}
						}
						// add all extra slices that were added as needing to be fixed
						for (int j = i; j < storedChecksumEntriesLength; j++) {
							badSlices.add(j);
						}
						
					} else {
						// local metadata had a slice deleted from it, need to find which slice it is
						for (int i = 0; i < storedChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
								// once the bad slice has been found, all subsequent lines will be corrupt also and need to simply be replaced
								for (int j = i; j < tempChecksumEntriesLength; j++) {
									badSlices.add(j);
								}
								break;
							}
						}
					}
					ChunkServerSendOnlyCorruptChunkToController corruptChunkSender = new ChunkServerSendOnlyCorruptChunkToController(chunkServerNodeInformation, chunknumber, filename, badSlices.size(), badSlices);
					this.controllerSender.sendData(corruptChunkSender.getBytes());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (DEBUG) { System.out.println("end ChunkServer handleControllerForwardDataToNewChunkServer"); }
	}
	
	private void handleControllerForwardFixCorruptChunkToChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleControllerForwardFixCorruptChunkToChunkServer"); }
		
		ControllerForwardFixCorruptChunkToChunkServer fixCorrupt = (ControllerForwardFixCorruptChunkToChunkServer) event;
		
		int chunknumber = fixCorrupt.getChunknumber();
		String filename = fixCorrupt.getFilename();
		
		String filelocation = this.tempFileLocationReplacement + filename + "_chunk" + chunknumber;
		
		File fileToReturn = new File(filelocation);
		
		if (fileToReturn.exists()) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				Metadata tempMetadata = new Metadata(1, chunknumber);
				
				Metadata storedMetadata = filesWithMetadataMap.get(filelocation);
				String storedChecksum = storedMetadata.getChecksum();
				
				tempMetadata.generataSHA1Checksum(tempData, SIZE_OF_SLICE);
				
				if (tempMetadata.getChecksum().equals(storedChecksum)) {
					// success, requested data is same as the one stored on this system
					// forward to the client like normal
					NodeInformation client = fixCorrupt.getClient();
					int totalNumberOfChunks = fixCorrupt.getTotalNumberOfChunks();
					
					Socket clientServer = new Socket(client.getNodeIPAddress(), client.getNodePortNumber());
					
					TCPSender clientSender = new TCPSender(clientServer);
					
					ChunkServerSendChunkToClient chunkToSend = new ChunkServerSendChunkToClient(tempData, chunknumber, filename, totalNumberOfChunks);
					
					clientSender.sendData(chunkToSend.getBytes());
					
					// now find the correct slices to forward that data to the chunk server missing those slices
					int numberOfBadSlices = fixCorrupt.getNumberOfBadSlices();
					byte[] fixedSlices = new byte[numberOfBadSlices];
					
					ArrayList<Integer> badslices = fixCorrupt.getBadSlices();
					
					int index = 0;
					for (Integer i : badslices) {
						if (i < tempData.length) {
							fixedSlices[index] = tempData[i];
							index ++;
						}
					}
					
					int numberOfDataStored = tempData.length;
					
					long timestamp = fileToReturn.lastModified();
					
					ChunkServerFixCorruptChunkToChunkServer fixedChunk = new ChunkServerFixCorruptChunkToChunkServer(fixedSlices, chunknumber, filename, timestamp, numberOfBadSlices, badslices, numberOfDataStored);
					
					NodeInformation corruptChunkServer = fixCorrupt.getChunkServer();
					
					Socket corruptChunkServerSocket = new Socket(corruptChunkServer.getNodeIPAddress(), corruptChunkServer.getNodePortNumber());
					TCPSender chunkSender = new TCPSender(corruptChunkServerSocket);
					chunkSender.sendData(fixedChunk.getBytes());
					
				} else {
					// data has been messed with in some way
					System.out.println("Data has been corrupted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
					
					String[] storedChecksumEntries = storedChecksum.split("\n");
					String[] tempChecksumEntries = tempMetadata.getChecksum().split("\n");
					
					int storedChecksumEntriesLength = storedChecksumEntries.length;
					int tempChecksumEntriesLength = tempChecksumEntries.length;
					
					System.out.println("Stored Checksum Length: " + storedChecksumEntriesLength);
					System.out.println("Temp Checksum Length: " + tempChecksumEntriesLength);
					
					ArrayList<Integer> badSlices = new ArrayList<Integer>();
					
					// local metadata had a slice added to it or possibly modified one line
					if (storedChecksumEntriesLength >= tempChecksumEntriesLength) {
						int i;
						// loop through the entries that are there to find corrupt slices
						for (i = 0; i < tempChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
							}
						}
						// add all extra slices that were added as needing to be fixed
						for (int j = i; j < storedChecksumEntriesLength; j++) {
							badSlices.add(j);
						}
						
					} else {
						// local metadata had a slice deleted from it, need to find which slice it is
						for (int i = 0; i < storedChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
								// once the bad slice has been found, all subsequent lines will be corrupt also and need to simply be replaced
								for (int j = i; j < tempChecksumEntriesLength; j++) {
									badSlices.add(j);
								}
								break;
							}
						}
					}
					ChunkServerSendOnlyCorruptChunkToController corruptChunkSender = new ChunkServerSendOnlyCorruptChunkToController(chunkServerNodeInformation, chunknumber, filename, badSlices.size(), badSlices);
					this.controllerSender.sendData(corruptChunkSender.getBytes());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// chunk has been deleted, need to report it to the controller and get missing chunk
			System.out.println("Data has been deleted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
			try {
				synchronized (filesWithChunkNumberMap) {
					filesWithChunkNumberMap.get(filename).remove(chunknumber);
				}
				ChunkServerDeletedChunkToController deletedChunk = new ChunkServerDeletedChunkToController(chunkServerNodeInformation, chunknumber, filename);
				this.controllerSender.sendData(deletedChunk.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end ChunkServer handleControllerForwardFixCorruptChunkToChunkServer"); }
	}
	
	private void handleControllerForwardOnlyFixCorruptChunkToChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleControllerForwardOnlyFixCorruptChunkToChunkServer"); }
		
	ControllerForwardOnlyFixCorruptChunkToChunkServer fixCorrupt = (ControllerForwardOnlyFixCorruptChunkToChunkServer) event;
		
		int chunknumber = fixCorrupt.getChunknumber();
		String filename = fixCorrupt.getFilename();
		
		String filelocation = this.tempFileLocationReplacement + filename + "_chunk" + chunknumber;
		
		File fileToReturn = new File(filelocation);
		
		if (fileToReturn.exists()) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				Metadata tempMetadata = new Metadata(1, chunknumber);
				
				Metadata storedMetadata = filesWithMetadataMap.get(filelocation);
				String storedChecksum = storedMetadata.getChecksum();
				
				tempMetadata.generataSHA1Checksum(tempData, SIZE_OF_SLICE);
				
				if (tempMetadata.getChecksum().equals(storedChecksum)) {
					// success, requested data is same as the one stored on this system

					// find the correct slices to forward that data to the chunk server missing those slices
					int numberOfBadSlices = fixCorrupt.getNumberOfBadSlices();
					byte[] fixedSlices = new byte[numberOfBadSlices];
					
					ArrayList<Integer> badslices = fixCorrupt.getBadSlices();
					
					int index = 0;
					for (Integer i : badslices) {
						if (i < tempData.length) {
							fixedSlices[index] = tempData[i];
							index ++;
						}
					}
					
					int numberOfDataStored = tempData.length;
					
					long timestamp = fileToReturn.lastModified();
					
					ChunkServerFixCorruptChunkToChunkServer fixedChunk = new ChunkServerFixCorruptChunkToChunkServer(fixedSlices, chunknumber, filename, timestamp, numberOfBadSlices, badslices, numberOfDataStored);
					
					NodeInformation corruptChunkServer = fixCorrupt.getChunkServer();
					
					Socket corruptChunkServerSocket = new Socket(corruptChunkServer.getNodeIPAddress(), corruptChunkServer.getNodePortNumber());
					TCPSender chunkSender = new TCPSender(corruptChunkServerSocket);
					chunkSender.sendData(fixedChunk.getBytes());
					
				} else {
					// data has been messed with in some way
					System.out.println("Data has been corrupted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
					
					String[] storedChecksumEntries = storedChecksum.split("\n");
					String[] tempChecksumEntries = tempMetadata.getChecksum().split("\n");
					
					int storedChecksumEntriesLength = storedChecksumEntries.length;
					int tempChecksumEntriesLength = tempChecksumEntries.length;
					
					System.out.println("Stored Checksum Length: " + storedChecksumEntriesLength);
					System.out.println("Temp Checksum Length: " + tempChecksumEntriesLength);
					
					ArrayList<Integer> badSlices = new ArrayList<Integer>();
					
					// local metadata had a slice added to it or possibly modified one line
					if (storedChecksumEntriesLength >= tempChecksumEntriesLength) {
						int i;
						// loop through the entries that are there to find corrupt slices
						for (i = 0; i < tempChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
							}
						}
						// add all extra slices that were added as needing to be fixed
						for (int j = i; j < storedChecksumEntriesLength; j++) {
							badSlices.add(j);
						}
						
					} else {
						// local metadata had a slice deleted from it, need to find which slice it is
						for (int i = 0; i < storedChecksumEntriesLength; i++) {
							if (!storedChecksumEntries[i].equals(tempChecksumEntries[i])) {
								badSlices.add(i);
								// once the bad slice has been found, all subsequent lines will be corrupt also and need to simply be replaced
								for (int j = i; j < tempChecksumEntriesLength; j++) {
									badSlices.add(j);
								}
								break;
							}
						}
					}
					ChunkServerSendOnlyCorruptChunkToController corruptChunkSender = new ChunkServerSendOnlyCorruptChunkToController(chunkServerNodeInformation, chunknumber, filename, badSlices.size(), badSlices);
					this.controllerSender.sendData(corruptChunkSender.getBytes());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// chunk has been deleted, need to report it to the controller and get missing chunk
			System.out.println("Data has been deleted, sending error report to Controller and removing ChunkServer from available servers for this data. Please request another ChunkServer");
			try {
				synchronized (filesWithChunkNumberMap) {
					filesWithChunkNumberMap.get(filename).remove(chunknumber);
				}
				ChunkServerDeletedChunkToController deletedChunk = new ChunkServerDeletedChunkToController(chunkServerNodeInformation, chunknumber, filename);
				this.controllerSender.sendData(deletedChunk.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end ChunkServer handleControllerForwardOnlyFixCorruptChunkToChunkServer"); }
	}
	
	private void handleChunkServerFixCorruptChunkToChunkServer(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleChunkServerFixCorruptChunkToChunkServer"); }
		
		ChunkServerFixCorruptChunkToChunkServer fixedChunk = (ChunkServerFixCorruptChunkToChunkServer) event;
		
		byte[] fixedBytes = fixedChunk.getChunkBytes();
		
		String filename = fixedChunk.getFilename();
		int chunknumber = fixedChunk.getChunkNumber();
		ArrayList<Integer> badslices = fixedChunk.getBadSlices();
		int numberOfDataStored = fixedChunk.getNumberOfDataStored();
		
		String filelocation = this.tempFileLocationReplacement + filename + "_chunk" + chunknumber;
		
		File fileToReturn = new File(filelocation);
		
		if (fileToReturn.exists()) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				// using the number of data stored will build a byte array that should hold all of the correct values now
				// just need to pull the correct data from either the original array or the new array
				byte[] combinedBytes = new byte[numberOfDataStored];
				
				int fixedBytesCounter = 0;
				int tempBytesCounter = 0;
				
				for (int i = 0; i < numberOfDataStored; i++) {
					if (badslices.contains(i)) {
						combinedBytes[i] = fixedBytes[fixedBytesCounter];
						fixedBytesCounter++;
					} else {
						combinedBytes[i] = tempData[tempBytesCounter];
						tempBytesCounter++;
					}
				}
				
				Metadata storedMetadata = filesWithMetadataMap.get(filelocation);
				int versionNumber = storedMetadata.getVersionInfoNumber() + 1;
				
				saveFile(filename, combinedBytes, chunknumber, versionNumber);
				
				// consider telling the controller here that the data has been healed and that the chunk server can be added back to being ana availbale chunk server
				ChunkServerNotifyFixSuccessToController fixSuccess = new ChunkServerNotifyFixSuccessToController(chunkServerNodeInformation, chunknumber, filename);
				this.controllerSender.sendData(fixSuccess.getBytes());
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (DEBUG) { System.out.println("end ChunkServer handleChunkServerFixCorruptChunkToChunkServer"); }
	}
	
	private void saveFile(String fileName, byte[] chunkData, int chunkNumber, int versionNumber) {
		//String fileAbsolutePath = FILE_LOCATION + fileName;
		String path = this.tempFileLocationReplacement + fileName + "_chunk" + chunkNumber;
		File fileLocationToBeSaved = new File(path.substring(0, path.lastIndexOf("/")));
		
		if (!fileLocationToBeSaved.exists()) {
			fileLocationToBeSaved.mkdirs();
		}
		
		File fileToBeSaved = new File(path);
		
		try {
			// save file to the local system
			FileOutputStream fos = new FileOutputStream(fileToBeSaved);
			fos.write(chunkData, 0, chunkData.length);
			
			System.out.println("Saving file to the following location: " + fileToBeSaved.getAbsolutePath());
			
			
			// generate metadata to write for saving in a different file
			Metadata metadata = new Metadata(versionNumber, chunkNumber);
			
			// encrypt the data to create the checksum
			metadata.generataSHA1Checksum(chunkData, SIZE_OF_SLICE);
			
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
			
			System.out.println("Saving metadata to the following location: " + metadataFile.getAbsolutePath());
			
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
