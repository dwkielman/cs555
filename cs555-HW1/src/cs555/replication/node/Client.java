package cs555.replication.node;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import cs555.replication.transport.TCPReceiverThread;
import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ClientChunkServerRequestToController;
import cs555.replication.wireformats.ClientRegisterRequestToController;
import cs555.replication.wireformats.ControllerRegisterResponseToClient;
import cs555.replication.wireformats.Event;
import cs555.replication.wireformats.Protocol;

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
	private TCPSender clientSender;
	
	private static final int SIZE_OF_CHUNK = 1024 * 64;
	
	private Client(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		
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
			// REGISTER_RESPONSE = 6001
			case Protocol.CONTROLLER_REGISTER_RESPONSE_TO_CLIENT:
				handleControllerRegisterResponse(event);	
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
						client.sendClientChunkServerRequestToController(file);
					} else {
						System.out.println("Command unrecognized. Please enter a valid input.");
					}
            		
            		break;
            	case "R":
            		if (DEBUG) { System.out.println("User selected Read a file."); }
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
			
			this.clientSender = new TCPSender(controllerSocket);
			
			ClientRegisterRequestToController clientRegisterRequest = new ClientRegisterRequestToController(this.controllerNodeInformation.getNodeIPAddress(), this.controllerNodeInformation.getNodePortNumber());

			if (DEBUG) { System.out.println("ChunkServer about to send message type: " + clientRegisterRequest.getType()); }
			
			this.clientSender.sendData(clientRegisterRequest.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end Client connectToController"); }
	}
	
	private void handleControllerRegisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin ChunkServer handleChunkServerRegisterResponse"); }
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
		if (DEBUG) { System.out.println("end ChunkServer handleChunkServerRegisterResponse"); }
	}
	
	private void sendClientChunkServerRequestToController(File file) {
		byte[] chunkSizeBytes = new byte[SIZE_OF_CHUNK];
		int fileLength = 0;
		int chunkNumber = 0;
		
		NodeInformation client = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
		
		// current idea on how to implement this:
		
		// step 1: create a class that splits the file up into chunks with the corresponding bytes for said chunk. Store that in some global variable here.
		
		// step 2: Once that object has been created, send the 0 chunk to the controller to request for servers for that chunk. Perhaps set some global variable at this point to not allow writing anything in the command line
		
		// step 3: in the metadata of what is sent back and forth, need to include the chunk number that we're on and the total number of chunks
		
		// step 4: controller sends chunk servers, listen in client for those to come in and it will say which chunk it is sending. Use that to get the correct byte data from the split file and send to one chunk server along with a list of other chunk servers
		
		// step 5: if chunk number is not equal to the total number of chunks, send another request to the controller for the next chunk number to be written
		
		// step 6: repeat process until chunk number == total number of chunks. In that case, file has been stored and can set the command line variable to true again to allow interfacting
		
		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			
			while ((fileLength = bis.read(chunkSizeBytes)) >= 0) {
				ClientChunkServerRequestToController chunkServersRequest = new ClientChunkServerRequestToController(client, chunkNumber, file.getName());
				
				this.clientSender.sendData(chunkServersRequest.getBytes());
				
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	
		
}
