package cs555.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import cs555.transport.TCPReceiverThread;
import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.NodeInformation;
import cs555.wireformats.Event;

public class StoreData implements Node {
	
	private static boolean DEBUG = false;
	private NodeInformation discoveryNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	private TCPReceiverThread storeDataTCPReceiverThread;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender discoverySender;
	private NodeInformation storeDataNodeInformation;
	private static StoreData storeData;
	
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
		// Once the initialization is complete, client should send a registration request to the controller.
		//connectToDiscovery();
	}
	
	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event " + eventType + " Passed to Client."); }
		switch(eventType) {
		/**
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
			System.out.println("Invalid argument. Second argument must be a number.");
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
					File file = new File(filename);
					if (file.exists()) {
						//this.accessUserInput = false;
						/**
						int chunkNumber = 0;
						long timestamp = file.lastModified();
						this.fileIntoChunks = splitFileIntoBytes(file, chunkNumber);
						sendClientChunkServerRequestToController(filename, chunkNumber, timestamp);
						**/
					} else {
						System.out.println("File does not exist. Please try again.");
					}
            		break;
            	case "R":
            		if (DEBUG) { System.out.println("User selected Read a file."); }
            		String filenameToRead;
            		System.out.println("Enter the name of the file that you wish to read: ");
            		filenameToRead = scan.nextLine();
            		//this.accessUserInput = false;
            		/**
            		sendClientReadFileRequestToController(filenameToRead, 0);
            		**/
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
	
	/**
	private void connectToDiscovery() {
		if (DEBUG) { System.out.println("begin StoreData connectToDiscovery"); }
		try {
			System.out.println("Attempting to connect to Discovery " + this.controllerNodeInformation.getNodeIPAddress() + " at Port Number: " + this.controllerNodeInformation.getNodePortNumber());
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
	**/
}
