package cs555.replication.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerRegisterRequest;
import cs555.replication.wireformats.ControllerRegisterResponseChunkServer;
import cs555.replication.wireformats.Event;
import cs555.replication.wireformats.Protocol;

/**
 * A controller node for managing information about chunk servers and chunks within the
 * system. There will be only 1 instance of the controller node.
 *
 */

public class Controller implements Node {

	private static final boolean DEBUG = false;
	private int portNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private HashMap<NodeInformation, TCPSender> chunkServerNodesMap;
	
	private Controller(int portNumber) {
		this.portNumber = portNumber;
		this.chunkServerNodesMap = new HashMap<NodeInformation, TCPSender>();
		
		try {
			TCPServerThread controllerServerThread = new TCPServerThread(this.portNumber, this);
			this.tCPServerThread = controllerServerThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			System.out.println("Controller TCPServerThread running.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a controller
		if(args.length != 1) {
            System.out.println("Invalid Arguments. Must include a port number.");
            return;
        }
		
		int controllerPortNumber = 0;
		
		try {
			controllerPortNumber = Integer.parseInt(args[0]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Argument must be a number.");
			nfe.printStackTrace();
		}
		
		Controller controller = new Controller(controllerPortNumber);
		
		String controllerIP = "";
		
        try{
        	controllerIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Controller is running at IP Address: " + controllerIP + " on Port Number: " + controller.portNumber);
        //handleUserInput(controller);
	}
	
	@Override
	public synchronized void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event Type " + eventType + " passed to Registry."); }
		switch(eventType) {
			// CHUNKSERVER_REGISTER_REQUEST = 6000
			case Protocol.CHUNKSERVER_REGISTER_REQUEST:
				handleChunkServerRegisterRequest(event);
				break;
		}
		
	}

	@Override
	public void setLocalHostPortNumber(int localPort) {
		this.portNumber = localPort;
		
	}
	
	public void handleChunkServerRegisterRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleChunkServerRegisterRequest"); }
		ChunkServerRegisterRequest chunkServerRegisterRequest = (ChunkServerRegisterRequest) event;
		String IP = chunkServerRegisterRequest.getIPAddress();
		int port = chunkServerRegisterRequest.getPortNumber();
		
		if (DEBUG) { System.out.println("Controller received a message type: " + chunkServerRegisterRequest.getType()); }
		
		System.out.println("Controller received a chunkServerRegisterRequest from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);
		
		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the map of nodes
			if (!this.chunkServerNodesMap.containsKey(ni)) {
				this.chunkServerNodesMap.put(ni, sender);
				System.out.println("Registration request successful. The number of messaging nodes currently constituting the Registry is (" + this.chunkServerNodesMap.size() + ")");
				status = (byte) 1;
				message = "Node Registered";
			} else {
				status = (byte) 0;
				message = "Node already registered. No action taken";
			}
			
			ControllerRegisterResponseChunkServer registerResponse = new ControllerRegisterResponseChunkServer(status, message);
			sender.sendData(registerResponse.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Registry handleChunkServerRegisterRequest"); }
	}

}
