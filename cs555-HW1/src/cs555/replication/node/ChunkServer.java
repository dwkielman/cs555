package cs555.replication.node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import cs555.replication.transport.TCPReceiverThread;
import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerRegisterRequestToController;
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
	private final String FILE_LOCATION = "/tmp/data";
	private TCPReceiverThread chunkServerTCPReceiverThread;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender chunkServerSender;

	private ChunkServer(String controllerIPAddress, int controllerPortNumber) {
		this.controllerNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.clientNodesMap = new HashMap<NodeInformation, TCPSender>();
		
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

}
