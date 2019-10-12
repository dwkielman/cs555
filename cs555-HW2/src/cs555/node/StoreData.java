package cs555.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Logger;

import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.HexConverter;
import cs555.util.NodeInformation;
import cs555.wireformats.DiscoveryReadRequestResponseToStoreData;
import cs555.wireformats.DiscoveryStoreRequestResponseToStoreData;
import cs555.wireformats.Event;
import cs555.wireformats.PeerSendFileToStoreData;
import cs555.wireformats.PeerStoreDestinationToStoreData;
import cs555.wireformats.Protocol;
import cs555.wireformats.StoreDataReadRequestToDiscovery;
import cs555.wireformats.StoreDataSendFileToPeer;
import cs555.wireformats.StoreDataSendReadRequestToPeer;
import cs555.wireformats.StoreDataSendStoreRequestToPeer;
import cs555.wireformats.StoreDataStoreRequestToDiscovery;

public class StoreData implements Node {
	
	private static boolean DEBUG = false;
	private NodeInformation discoveryNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	//private TCPSender discoverySender;
	private NodeInformation storeDataNodeInformation;
	private static StoreData storeData;
	private final static Logger LOGGER = Logger.getLogger(Discovery.class.getName());
	
	private static final int SIZE_OF_CHUNK = 1024 * 64;
	
	private StoreData(String discoveryIPAddress, int discoveryPortNumber) {
		this.discoveryNodeInformation = new NodeInformation(discoveryIPAddress, discoveryPortNumber);
		
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
		this.storeDataNodeInformation = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
	}
	
	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event " + eventType + " Passed to Client."); }
		switch(eventType) {
			// DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA = 6002
			case Protocol.DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA:
				handleDiscoveryStoreRequestResponseToStoreData(event);
				break;
			// DISCOVERY_READ_REQUEST_RESPONSE_TO_STOREDATA = 6003
			case Protocol.DISCOVERY_READ_REQUEST_RESPONSE_TO_STOREDATA:
				handleDiscoveryReadRequestResponseToStoreData(event);
				break;
			// PEER_STORE_DESTINATION_TO_STORE_DATA = 7007
			case Protocol.PEER_STORE_DESTINATION_TO_STORE_DATA:
				handlePeerStoreDestinationToStoreData(event);
				break;
			// PEER_SEND_FILE_TO_STORE_DATA = 7009
			case Protocol.PEER_SEND_FILE_TO_STORE_DATA:
				handlePeerSendFileToStoreData(event);
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
            System.out.println("Invalid Arguments. Must include discovery host name and port number.");
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
		
		String discoveryIPAddress = args[0];
		int discoveryPortNumber = 0;
		
		try {
			discoveryPortNumber = Integer.parseInt(args[1]);
		} catch (NumberFormatException nfe) {
			//System.out.println("Invalid argument. Second argument must be a number.");
			LOGGER.severe("Invalid argument. Second argument must be a number.");
			nfe.printStackTrace();
		}
		
		StoreData storeData = new StoreData(discoveryIPAddress, discoveryPortNumber);
		
		storeData.handleUserInput();
	}
	
	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Ready for input.");
			
        while(true) {
            System.out.println("Options:\n[S] Store a File\n[R] Read a File\n[Q] Quit\nPlease type your request: ");
            String input = scan.nextLine();
            
            input = input.toUpperCase();
            switch (input) {
            	case "S":
            		if (DEBUG) { System.out.println("User selected Store a file."); }
            		String filename;
            		System.out.println("Enter the name of the file that you wish to store: ");
					filename = scan.nextLine();
					
					System.out.println("Enter the name of the ID that you wish to use (Leave blank to auto-generate an ID): ");
					String id = scan.nextLine();
					byte[] idBytes;
					
					if (!id.equals("")) {
						idBytes = HexConverter.convertHexToBytes(id);
					} else {
						try {
							id = autogenerateKey(filename);
							idBytes = HexConverter.convertHexToBytes(id);
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						}
					}
					
					File file = new File(filename);
					if (file.exists()) {
						sendStoreDataStoreRequestToDiscovery(filename, id);
					} else {
						System.out.println("File does not exist. Please try again.");
					}
            		break;
            	case "R":
            		if (DEBUG) { System.out.println("User selected Read a file."); }
            		String filenameToRead;
            		System.out.println("Enter the name of the file that you wish to read: ");
            		filenameToRead = scan.nextLine();
            		System.out.println("Enter the name of the ID that you wish to use (Leave blank to auto-generate an ID): ");
					String idRead = scan.nextLine();
					byte[] idReadBytes;
					
					if (!idRead.equals("")) {
						idReadBytes = HexConverter.convertHexToBytes(idRead);
					} else {
						try {
							idRead = autogenerateKey(filenameToRead);
							idReadBytes = HexConverter.convertHexToBytes(idRead);
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						}
					}
					sendStoreDataReadRequestToDiscovery(filenameToRead, idRead);
					
            		break;
            	case "Q":
            		if (DEBUG) { System.out.println("User selected Quit."); }
            		System.out.println("Quitting program. Goodbye.");
            		System.exit(1);
            	default:
            		System.out.println("Command unrecognized. Please enter a valid input.");
            }
        }
	}
	
	private void sendStoreDataStoreRequestToDiscovery(String filename, String key) {
		if (DEBUG) { System.out.println("begin StoreData sendStoreDataStoreRequestToDiscovery"); }
		
		try {
			StoreDataStoreRequestToDiscovery storeRequest = new StoreDataStoreRequestToDiscovery(this.storeDataNodeInformation, filename, key);
			Socket socket = new Socket(this.discoveryNodeInformation.getNodeIPAddress(), this.discoveryNodeInformation.getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			sender.sendData(storeRequest.getBytes());

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end StoreData sendStoreDataStoreRequestToDiscovery"); }
	}
	
	private void handleDiscoveryStoreRequestResponseToStoreData(Event event) {
		if (DEBUG) { System.out.println("begin StoreData handleDiscoveryStoreRequestResponseToStoreData"); }
		DiscoveryStoreRequestResponseToStoreData storeResponse = (DiscoveryStoreRequestResponseToStoreData) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + storeResponse.getType()); }
		
		NodeInformation peer = storeResponse.getPeer();
		String filename = storeResponse.getFilename();
		String key = storeResponse.getKey();
		
		ArrayList<String> traceList = new ArrayList<String>();
		traceList.add("StoreData");
		int hopCount = 0;
		//System.out.println("Initiating Lookup for Storing a file. Discovery has sent Random Node: " + peer);
		//System.out.println("File to be Stored: " + filename);
		//System.out.println("Assigned File Key: " + key);
		
		LOGGER.info("Initiating Lookup to Store a file. Discovery has sent Random Node: " + peer);
		LOGGER.info("File to be Stored: " + filename);
		LOGGER.info("Assigned File Identifier: " + key);
		
		StoreDataSendStoreRequestToPeer storeRequestToPeer = new StoreDataSendStoreRequestToPeer(filename, key, this.storeDataNodeInformation, traceList.size(), traceList, hopCount);
		
		try {
			Socket socket = new Socket(peer.getNodeIPAddress(), peer.getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			sender.sendData(storeRequestToPeer.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		if (DEBUG) { System.out.println("end StoreData handleDiscoveryStoreRequestResponseToStoreData"); }
	}
	
	private void handlePeerStoreDestinationToStoreData(Event event) {
		if (DEBUG) { System.out.println("begin StoreData handlePeerStoreDestinationToStoreData"); }
		PeerStoreDestinationToStoreData storeDestination = (PeerStoreDestinationToStoreData) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + storeDestination.getType()); }
		
		NodeInformation peer = storeDestination.getPeer();
		String filename = storeDestination.getFilename();
		String key = storeDestination.getKey();
		ArrayList<String> traceList = storeDestination.getTraceList();
		int hopCount = storeDestination.getHopCount();
		
		//System.out.println("Lookup Copmplete for Storing a file. Destination Peer: " + peer);
		LOGGER.info("Lookup Copmplete for Storing a file. Destination Peer: " + peer);
		String traceString = "";
        for (String s : traceList) {
        	traceString = traceString + s + "-";
        }
        
        traceString = traceString.substring(0, traceString.length() - 1);
        
        //System.out.println("Trace: " + traceString);
        //System.out.println("Hop Count: " + hopCount);
        LOGGER.info("Trace: " + traceString);
        LOGGER.info("Hop Count: " + hopCount);
		
		File file = new File(filename);
		if (file.exists()) {
			try {
				byte[] filebytes = new byte[(int) file.length()];
				FileInputStream fis = new FileInputStream(file);
				fis.read(filebytes);
				fis.close();
				
				StoreDataSendFileToPeer sendFile = new StoreDataSendFileToPeer(filename, key, filebytes);
				
				Socket socket = new Socket(peer.getNodeIPAddress(), peer.getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				
				sender.sendData(sendFile.getBytes());
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end StoreData handlePeerStoreDestinationToStoreData"); }
	}
	
	private void sendStoreDataReadRequestToDiscovery(String filename, String key) {
		if (DEBUG) { System.out.println("begin StoreData sendStoreDataReadRequestToDiscovery"); }
		
		try {
			StoreDataReadRequestToDiscovery readRequest = new StoreDataReadRequestToDiscovery(this.storeDataNodeInformation, filename, key);
			Socket socket = new Socket(this.discoveryNodeInformation.getNodeIPAddress(), this.discoveryNodeInformation.getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			sender.sendData(readRequest.getBytes());

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end StoreData sendStoreDataReadRequestToDiscovery"); }
	}
	
	private void handleDiscoveryReadRequestResponseToStoreData(Event event) {
		if (DEBUG) { System.out.println("begin StoreData handleDiscoveryReadRequestResponseToStoreData"); }
		DiscoveryReadRequestResponseToStoreData readResponse = (DiscoveryReadRequestResponseToStoreData) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + readResponse.getType()); }
		
		NodeInformation peer = readResponse.getPeer();
		String filename = readResponse.getFilename();
		String key = readResponse.getKey();
		
		ArrayList<String> traceList = new ArrayList<String>();
		traceList.add("StoreData");
		int hopCount = 0;
		
		//System.out.println("Initiating Lookup to Read a file. Discovery has sent Random Node: " + peer);
		//System.out.println("File to be Read: " + filename);
		//System.out.println("Assigned File Key: " + key);
		LOGGER.info("Initiating Lookup to Read a file. Discovery has sent Random Node: " + peer);
		LOGGER.info("File to be Read: " + filename);
		LOGGER.info("Assigned File Identifier: " + key);
		
		StoreDataSendReadRequestToPeer readRequestToPeer = new StoreDataSendReadRequestToPeer(filename, key, this.storeDataNodeInformation, traceList.size(), traceList, hopCount);
		
		try {
			Socket socket = new Socket(peer.getNodeIPAddress(), peer.getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			
			sender.sendData(readRequestToPeer.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		if (DEBUG) { System.out.println("end StoreData handleDiscoveryReadRequestResponseToStoreData"); }
	}
	
	private void handlePeerSendFileToStoreData(Event event) {
		if (DEBUG) { System.out.println("begin StoreData handlePeerSendFileToStoreData"); }
		PeerSendFileToStoreData recievedFile = (PeerSendFileToStoreData) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + recievedFile.getType()); }
		
		NodeInformation peer = recievedFile.getPeer();
		String filename = recievedFile.getFilename();
		String key = recievedFile.getKey();
		ArrayList<String> traceList = recievedFile.getTraceList();
		int hopCount = recievedFile.getHopCount();
		byte[] receivedBytes = recievedFile.getFileBytes();
		
		//System.out.println("Lookup Copmplete for Reading a file. Destination Peer: " + peer);
		LOGGER.info("Lookup Copmplete for Reading a file. Destination Peer: " + peer);
		
		String traceString = "";
        for (String s : traceList) {
        	traceString = traceString + s + "-";
        }
        
        traceString = traceString.substring(0, traceString.length() - 1);
        
        //System.out.println("Trace: " + traceString);
        //System.out.println("Hop Count: " + hopCount);
        LOGGER.info("Trace: " + traceString);
        LOGGER.info("Hop Count: " + hopCount);
		
        saveFile(filename, receivedBytes);
		
		if (DEBUG) { System.out.println("end StoreData handlePeerSendFileToStoreData"); }
	}
	
	private void saveFile(String filename, byte[] data) {
		
		String path = "/tmp/received_dkielman/";
		File pathFile = new File(path);
		if (!pathFile.exists()) {
			pathFile.mkdir();
		}
		
		String receivedFileName = filename.substring(filename.lastIndexOf("/") + 1, filename.length());
		
		System.out.println("Attemping to write to the following directory:");
		System.out.println(path);
		
		String receivedFilePath = path + receivedFileName;
		System.out.println("Received Path is as follows: ");
		System.out.println(receivedFilePath);
		File receivedFile = new File(receivedFilePath);
		try {
			if (!receivedFile.exists()) {
					receivedFile.createNewFile();
			} 
			
			FileOutputStream fos = new FileOutputStream(receivedFile);
			fos.write(data, 0, data.length);
			
			fos.close();
			System.out.println("SUCCESS: File has been saved to the following location: " + pathFile.getAbsolutePath());
			System.out.println("Finished saving file: " + filename);
		} catch (IOException e) {
			System.out.println("FAILURE: An error has occurred while attempting to save the received file.");
			e.printStackTrace();
		}
	}
	
	private String autogenerateKey(String filename) throws NoSuchAlgorithmException {
		MessageDigest cript = MessageDigest.getInstance("SHA-1");
		cript.reset();
		cript.update(filename.getBytes());

		StringBuffer sb = new StringBuffer("");

		byte[] messageDataBytes = cript.digest();
		for (int i = 0; i < messageDataBytes.length; i++) {
			sb.append(Integer.toString((messageDataBytes[i] & 0xff) + 0x100, 16));
		}
		String keyString = "" + sb.toString().charAt(1)
				+ sb.toString().charAt(2) + sb.toString().charAt(4)
				+ sb.toString().charAt(8);

		return keyString.toUpperCase();
	}
}
