package cs555.replication.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.HeartbeatMetadata;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerRegisterRequestToController;
import cs555.replication.wireformats.ClientChunkServerRequestToController;
import cs555.replication.wireformats.ClientRegisterRequestToController;
import cs555.replication.wireformats.ControllerChunkServersResponseToClient;
import cs555.replication.wireformats.ControllerRegisterResponseToChunkServer;
import cs555.replication.wireformats.ControllerRegisterResponseToClient;
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
	private HashMap<NodeInformation, TCPSender> clientNodesMap;
	private ArrayList<HeartbeatMetadata> chunkServerHeartbeatMetadaList;
	private ArrayList<String> filenames;
	private HashMap<String, HashMap<Integer, ArrayList<HeartbeatMetadata>>> filesOnChunkServersMap;
	private final int REPLICATION_LEVEL = 3;
	
	private Controller(int portNumber) {
		this.portNumber = portNumber;
		this.chunkServerNodesMap = new HashMap<NodeInformation, TCPSender>();
		this.chunkServerHeartbeatMetadaList = new ArrayList<HeartbeatMetadata>();
		this.filesOnChunkServersMap = new HashMap<String, HashMap<Integer, ArrayList<HeartbeatMetadata>>>();
		this.filenames = new ArrayList<String>();
		
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
			// CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER = 7000
			case Protocol.CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER:
				handleChunkServerRegisterRequest(event);
				break;
			// CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8000
			case Protocol.CLIENT_REGISTER_REQUEST_TO_CONTROLLER:
				handleClientRegisterRequest(event);
				break;
			// CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER = 8001
			case Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER:
				handleClientChunkServerRequest(event);
				break;
			default:
				System.out.println("Invalid Event to Node.");
				return;
		}
	}

	@Override
	public void setLocalHostPortNumber(int localPort) {
		this.portNumber = localPort;
		
	}
	
	private void handleChunkServerRegisterRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleChunkServerRegisterRequest"); }
		ChunkServerRegisterRequestToController chunkServerRegisterRequest = (ChunkServerRegisterRequestToController) event;
		String IP = chunkServerRegisterRequest.getIPAddress();
		int port = chunkServerRegisterRequest.getPortNumber();
		long nodeFreeSpace = chunkServerRegisterRequest.getFreeSpace();
		
		
		if (DEBUG) { System.out.println("Controller received a message type: " + chunkServerRegisterRequest.getType()); }
		
		System.out.println("Controller received a chunkServerRegisterRequest from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);
		HeartbeatMetadata hbm = new HeartbeatMetadata(ni, 0, nodeFreeSpace);

		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the map of nodes
			if (!this.chunkServerNodesMap.containsKey(ni)) {
				this.chunkServerNodesMap.put(ni, sender);
				this.chunkServerHeartbeatMetadaList.add(hbm);
				System.out.println("Chunk Server Registration request successful. The number of Chunk Servers currently running on the Controller is (" + this.chunkServerNodesMap.size() + ")");
				status = (byte) 1;
				message = "Chunk Server Registered";
			} else {
				status = (byte) 0;
				message = "Chunk Server already registered. No action taken";
			}
			
			ControllerRegisterResponseToChunkServer chunkServerRegisterResponse = new ControllerRegisterResponseToChunkServer(status, message);
			sender.sendData(chunkServerRegisterResponse.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Controller handleChunkServerRegisterRequest"); }
	}

	private void handleClientRegisterRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleClientRegisterRequest"); }
		ClientRegisterRequestToController clientRegisterRequest = (ClientRegisterRequestToController) event;
		String IP = clientRegisterRequest.getIPAddress();
		int port = clientRegisterRequest.getPortNumber();
		
		if (DEBUG) { System.out.println("Controller received a message type: " + clientRegisterRequest.getType()); }
		
		System.out.println("Controller received a clientRegisterRequest from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);
		
		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the map of nodes
			if (!this.clientNodesMap.containsKey(ni)) {
				this.clientNodesMap.put(ni, sender);
				System.out.println("Client Registration request successful. The number of Clients currently running on the Controller is (" + this.clientNodesMap.size() + ")");
				status = (byte) 1;
				message = "Client Registered";
			} else {
				status = (byte) 0;
				message = "Client already registered. No action taken";
			}
			
			ControllerRegisterResponseToClient clientRegisterResponse = new ControllerRegisterResponseToClient(status, message);
			sender.sendData(clientRegisterResponse.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Controller handleChunkServerRegisterRequest"); }
	}
	
	private void handleClientChunkServerRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleClientChunkServerRequest"); }
		ClientChunkServerRequestToController clientChunkServerRequest = (ClientChunkServerRequestToController) event;
		NodeInformation clientNode = clientChunkServerRequest.getClienNodeInformation();
		int chunkNumber = clientChunkServerRequest.getChunkNumber();
		String filename = clientChunkServerRequest.getFilename();
		
		if (DEBUG) { System.out.println("Controller received a message type: " + clientChunkServerRequest.getType()); }
		
		//Set<String> files = filesOnChunkServersMap.keySet();
		
		// file not already stored in the hashmap
		if (!filesOnChunkServersMap.get(filename).containsKey(chunkNumber)) {
			//HashMap<Integer, ArrayList<HeartbeatMetadata>> chunkServers = this.filesOnChunkServersMap.get(filename);
			if (chunkServerHeartbeatMetadaList.size() >= REPLICATION_LEVEL) {
				try {
					filenames.add(filename);
					
					// sort the chunk servers by those with the most space, this should do it in descending order
					chunkServerHeartbeatMetadaList.sort((h1, h2) -> Long.compare(h2.getFreeSpaceAvailable(), h1.getFreeSpaceAvailable()));
					ArrayList<HeartbeatMetadata> hbArrayList = chunkServerHeartbeatMetadaList.stream().limit(REPLICATION_LEVEL).collect(Collectors.toCollection(ArrayList::new));
					
					// update the chunkServers Hashmap
					HashMap<Integer, ArrayList<HeartbeatMetadata>> tempMap = new HashMap<Integer, ArrayList<HeartbeatMetadata>>(chunkNumber);
					tempMap.put(chunkNumber, hbArrayList);
					filesOnChunkServersMap.put(filename, tempMap);
					
					// prep chunkServers to send to Client
					ArrayList<NodeInformation> chunkServers = new ArrayList<NodeInformation>();
					
					for (HeartbeatMetadata hbm : hbArrayList) {
						chunkServers.add(hbm.getNodeInformation());
					}
					
					// put the chunkserver nodeinformation into a message and send to client
					ControllerChunkServersResponseToClient chunkServersResponse = new ControllerChunkServersResponseToClient(REPLICATION_LEVEL, chunkServers, chunkNumber, filename);

					this.clientNodesMap.get(clientNode).sendData(chunkServersResponse.getBytes());
				} catch  (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			// clean up, add to the relevant collections and be sure to update the free space available on the chunk servers that are being used
		}
		
	}
	
}
