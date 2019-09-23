package cs555.erasure.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.stream.Collectors;

import cs555.erasure.transport.TCPSender;
import cs555.erasure.transport.TCPServerThread;
import cs555.erasure.util.HeartbeatMetadata;
import cs555.erasure.util.Metadata;
import cs555.erasure.util.NodeInformation;
import cs555.erasure.wireformats.ChunkServerRegisterRequestToController;
import cs555.erasure.wireformats.ChunkServerSendMajorHeartbeatToController;
import cs555.erasure.wireformats.ChunkServerSendMinorHeartbeatToController;
import cs555.erasure.wireformats.ClientChunkServerRequestToController;
import cs555.erasure.wireformats.ClientReadFileRequestToController;
import cs555.erasure.wireformats.ClientRegisterRequestToController;
import cs555.erasure.wireformats.ControllerChunkServerToReadResponseToClient;
import cs555.erasure.wireformats.ControllerChunkServersResponseToClient;
import cs555.erasure.wireformats.ControllerRegisterResponseToChunkServer;
import cs555.erasure.wireformats.ControllerRegisterResponseToClient;
import cs555.erasure.wireformats.Event;
import cs555.erasure.wireformats.Protocol;

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
	private HashMap<NodeInformation, HeartbeatMetadata> chunkServerHeartbeatMetadaList;
	private ArrayList<NodeInformation> deadChunkServers;
	private HashMap<String, HashMap<Integer, HashMap<Integer, NodeInformation>>> filesWithChunksNodeInformationMap;
	private static final int TOTAL_SHARDS = 9;
	private static Controller controller;
	private ArrayList<NodeInformation> tempLiveNodes;
	
	private Controller(int portNumber) {
		this.portNumber = portNumber;
		this.chunkServerNodesMap = new HashMap<NodeInformation, TCPSender>();
		this.clientNodesMap = new HashMap<NodeInformation, TCPSender>();
		this.chunkServerHeartbeatMetadaList = new  HashMap<NodeInformation, HeartbeatMetadata>();
		this.deadChunkServers = new ArrayList<NodeInformation>();
		this.filesWithChunksNodeInformationMap = new HashMap<String, HashMap<Integer, HashMap<Integer, NodeInformation>>>();
		this.tempLiveNodes = new ArrayList<NodeInformation>();
		
		try {
			TCPServerThread controllerServerThread = new TCPServerThread(this.portNumber, this);
			this.tCPServerThread = controllerServerThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			System.out.println("Controller TCPServerThread running.");
			//startHeartBeat();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void startHeartBeat() {
		/**
		TCPControllerHeartbeat tCPControllerHeartbeat = new TCPControllerHeartbeat(this);
		Thread tCPControllerHeartBeatThread = new Thread(tCPControllerHeartbeat);
		tCPControllerHeartBeatThread.start();
		**/
	}
	
	public Controller() {}
	
	public void addDeadChunkServer(NodeInformation deadChunkServer) {
		synchronized (deadChunkServers) {
			deadChunkServers.add(deadChunkServer);
		}
	}
	
	public TCPSender getChunkServerSender(NodeInformation chunkServer) {
		synchronized (chunkServerNodesMap) {
			return chunkServerNodesMap.get(chunkServer);
		}
	}
	
	public Set<NodeInformation> getLiveChunkServers() {
		System.out.println("Getting Live Chunk Servers.");
		synchronized (chunkServerHeartbeatMetadaList) {
			if (!chunkServerHeartbeatMetadaList.isEmpty()) {
				return chunkServerHeartbeatMetadaList.keySet();
			} else {
				System.out.println("Heartbeats are empty.");
				return null;
			}
			
		}
	}
	
	public ArrayList<NodeInformation> getTempLiveChunkServers() {
		synchronized (tempLiveNodes) {
			return tempLiveNodes;
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
		
		String controllerIP = "";
		
        try{
        	controllerIP = InetAddress.getLocalHost().getHostAddress();
        	System.out.println("Trying to get controllerIP");
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }
        
        System.out.println("Controller is running at IP Address: " + controllerIP + " on Port Number: " + controllerPortNumber);
        controller = new Controller(controllerPortNumber);
	}
	
	@Override
	public synchronized void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event Type " + eventType + " passed to Controller."); }
		switch(eventType) {
			// CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER = 7000
			case Protocol.CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER:
				handleChunkServerRegisterRequest(event);
				break;
			// CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER = 7003
			case Protocol.CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER:
				handleChunkServerSendMajorHeartbeatToController(event);
				break;
			// CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER = 7004
			case Protocol.CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER:
				handleChunkServerSendMinorHeartbeatToController(event);
				break;
			// CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8000
			case Protocol.CLIENT_REGISTER_REQUEST_TO_CONTROLLER:
				handleClientRegisterRequest(event);
				break;
			// CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER = 8001
			case Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER:
				handleClientChunkServerRequest(event);
				break;
			// CLIENT_READ_REQUEST_TO_CONTROLLER = 8003
			case Protocol.CLIENT_READ_REQUEST_TO_CONTROLLER:
				handleClientReadRequest(event);
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
		HeartbeatMetadata hbm = new HeartbeatMetadata(ni, 0, nodeFreeSpace, 0);

		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the map of nodes
			synchronized (chunkServerNodesMap) {
				if (!this.chunkServerNodesMap.containsKey(ni)) {
					this.chunkServerNodesMap.put(ni, sender);
					this.chunkServerHeartbeatMetadaList.put(ni, hbm);
					this.tempLiveNodes.add(ni);
					System.out.println("Chunk Server Registration request successful. The number of Chunk Servers currently running on the Controller is (" + this.chunkServerNodesMap.size() + ")");
					status = (byte) 1;
					message = "Chunk Server Registered";
				} else {
					status = (byte) 0;
					message = "Chunk Server already registered. No action taken";
				}
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
			synchronized (clientNodesMap) {
				if (!this.clientNodesMap.containsKey(ni)) {
					this.clientNodesMap.put(ni, sender);
					System.out.println("Client Registration request successful. The number of Clients currently running on the Controller is (" + this.clientNodesMap.size() + ")");
					status = (byte) 1;
					message = "Client Registered";
				} else {
					status = (byte) 0;
					message = "Client already registered. No action taken";
				}
			}
			
			ControllerRegisterResponseToClient clientRegisterResponse = new ControllerRegisterResponseToClient(status, message);
			sender.sendData(clientRegisterResponse.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Controller handleChunkServerRegisterRequest"); }
	}
	
	private void handleChunkServerSendMajorHeartbeatToController(Event event) {
		//if (DEBUG) { System.out.println("begin Controller handleChunkServerSendMajorHeartbeatToController"); }
		
		ChunkServerSendMajorHeartbeatToController majorHeartbeat = (ChunkServerSendMajorHeartbeatToController) event;
		
		NodeInformation chunkServer = majorHeartbeat.getChunkServer();
		int totalNumberOfChunks = majorHeartbeat.getTotalNumberOfChunks();
		long freespaceAvailable = majorHeartbeat.getFreespaceAvailable();
		ArrayList<Metadata> metadataList = majorHeartbeat.getMetadataList();
		int totalNumberOfShards = majorHeartbeat.getTotalNumberOfShards();
		
		HeartbeatMetadata hbm = new HeartbeatMetadata(chunkServer, totalNumberOfChunks, freespaceAvailable, totalNumberOfShards);
		hbm.setMetadata(metadataList);
		
		synchronized (chunkServerHeartbeatMetadaList) {
			chunkServerHeartbeatMetadaList.put(chunkServer, hbm);
		}
		
		//if (DEBUG) { System.out.println("end Controller handleChunkServerSendMajorHeartbeatToController"); }
	}
	
	private void handleChunkServerSendMinorHeartbeatToController(Event event) {
		//if (DEBUG) { System.out.println("begin Controller handleChunkServerSendMinorHeartbeatToController"); }
		
		ChunkServerSendMinorHeartbeatToController minorHeartbeat = (ChunkServerSendMinorHeartbeatToController) event;
		
		NodeInformation chunkServer = minorHeartbeat.getChunkServer();
		int totalNumberOfChunks = minorHeartbeat.getTotalNumberOfChunks();
		long freespaceAvailable = minorHeartbeat.getFreespaceAvailable();
		ArrayList<Metadata> metadataList = minorHeartbeat.getMetadataList();
		int totalNumberOfShards = minorHeartbeat.getTotalNumberOfShards();
		
		HeartbeatMetadata hbm = new HeartbeatMetadata(chunkServer, totalNumberOfChunks, freespaceAvailable, totalNumberOfShards);

		synchronized (chunkServerHeartbeatMetadaList) {
			HeartbeatMetadata storedHBM = chunkServerHeartbeatMetadaList.get(chunkServer);
			ArrayList<Metadata> storedHBMMetadataList = storedHBM.getMetadataList();
			
			for (Metadata md : storedHBMMetadataList) {
				metadataList.add(md);
			}
			
			hbm.setMetadata(metadataList);
			chunkServerHeartbeatMetadaList.put(chunkServer, hbm);
		}
		
		//if (DEBUG) { System.out.println("end Controller handleChunkServerSendMinorHeartbeatToController"); }
	}
	
	private void handleClientChunkServerRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleClientChunkServerRequest"); }
		ClientChunkServerRequestToController clientChunkServerRequest = (ClientChunkServerRequestToController) event;
		NodeInformation clientNode = clientChunkServerRequest.getClientNodeInformation();
		int chunkNumber = clientChunkServerRequest.getChunkNumber();
		String filename = clientChunkServerRequest.getFilename();
		int totalNumberOfChunks = clientChunkServerRequest.getTotalNumberOfChunks();
		System.out.println("Chunk Number: " + chunkNumber + ", total number of chunks: " + totalNumberOfChunks);
		if (DEBUG) { System.out.println("Controller received a message type: " + clientChunkServerRequest.getType()); }

		if (chunkServerHeartbeatMetadaList.size() >= TOTAL_SHARDS) {
			try {
				// sort the chunk servers by those with the most space, this should do it in descending order
				ArrayList<HeartbeatMetadata> tempHbmList = new ArrayList<HeartbeatMetadata>();
				for (HeartbeatMetadata hbm : this.chunkServerHeartbeatMetadaList.values()) {
					tempHbmList.add(hbm);
				}
				tempHbmList.sort((h1, h2) -> Long.compare(h2.getFreeSpaceAvailable(), h1.getFreeSpaceAvailable()));

				if (DEBUG)
				{
					System.out.println("The file storage and respective chunk servers are: ");
					for (HeartbeatMetadata h : tempHbmList) {
						System.out.println(h.getNodeInformation().getNodeIPAddress() + " with space: " + h.getFreeSpaceAvailable());
					}
					
				}
				// get the 9 chunk servers with the most space
				ArrayList<HeartbeatMetadata> hbArrayList = tempHbmList.stream().limit(TOTAL_SHARDS).collect(Collectors.toCollection(ArrayList::new));
				
				// prep chunkServers to send to Client
				ArrayList<NodeInformation> chunkServers = new ArrayList<NodeInformation>();
				
				for (HeartbeatMetadata hbm : hbArrayList) {
					chunkServers.add(hbm.getNodeInformation());
				}
				
				HashMap<Integer, NodeInformation> tempServersMap = new HashMap<Integer, NodeInformation>();
				
				int shardNumber = 1;
				for (NodeInformation ni : chunkServers) {
					tempServersMap.put(shardNumber, ni);
					shardNumber++;
				}
				
				// update the chunkServers Hashmap
				synchronized (filesWithChunksNodeInformationMap) {
					if (filesWithChunksNodeInformationMap.containsKey(filename)) {	
						filesWithChunksNodeInformationMap.get(filename).put(chunkNumber, tempServersMap);
						System.out.println("Filenames and chunks with respective chunk servers are: ");
						for (String s : filesWithChunksNodeInformationMap.keySet()) {
							System.out.println("Filename: " + s);
							System.out.println("Chunk Number / shard number / Chunk Server: " + filesWithChunksNodeInformationMap.get(s));
						}
					} else {
						HashMap<Integer, HashMap<Integer, NodeInformation>> tempMap = new HashMap<Integer, HashMap<Integer, NodeInformation>>();
						tempMap.put(chunkNumber, tempServersMap);
						filesWithChunksNodeInformationMap.put(filename, tempMap);
					}
				}
				
				// put the chunkserver nodeinformation into a message and send to client
				ControllerChunkServersResponseToClient chunkServersResponse = new ControllerChunkServersResponseToClient(TOTAL_SHARDS, tempServersMap, chunkNumber, totalNumberOfChunks, filename);
				this.clientNodesMap.get(clientNode).sendData(chunkServersResponse.getBytes());
			} catch  (IOException ioe) {
				ioe.printStackTrace();
			}
		} else {
			System.out.println("Not Enough Chunk Servers to store the file on the system. Please try again at a laster time.");
		}
		if (DEBUG) { System.out.println("end Controller handleClientChunkServerRequest"); }
	}

	private void handleClientReadRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleClientReadRequest"); }
		
		ClientReadFileRequestToController readRequest = (ClientReadFileRequestToController) event;
		String filename = readRequest.getFilename();
		NodeInformation clientNode = readRequest.getClientNodeInformation();
		int chunkNumber = readRequest.getChunkNumber();
		
		try {
			// make sure that the file is stored on a chunkserver first
			synchronized (filesWithChunksNodeInformationMap) {
				if (filesWithChunksNodeInformationMap.containsKey(filename)) {
					// get the metadata for the chunkservers that hold the first value of the file
					// get the chunk servers associated with the file
					HashMap<Integer, NodeInformation> shardsWithChunkServerMap = new HashMap<Integer, NodeInformation>();
					
					System.out.println("Attempting to get " + filename + " with Chunk " + chunkNumber + " from chunk server in the map."); 
					
					shardsWithChunkServerMap = filesWithChunksNodeInformationMap.get(filename).get(chunkNumber);
					int totalNumberOfChunks = filesWithChunksNodeInformationMap.get(filename).size();
					int shardNumberWithChunkServerSize = shardsWithChunkServerMap.size();
					
					System.out.println("Total Number of Chunks: " + totalNumberOfChunks);
					System.out.println("Total Number of Shards: " + shardNumberWithChunkServerSize);
					
					// sned all fo the chunk servers back to the client
					if (!shardsWithChunkServerMap.isEmpty()) {
						// ControllerChunkServerToReadResponseToClient(int shardNumberWithChunkServerSize, HashMap<Integer, NodeInformation> shardNumberWithChunkServer, int chunkNumber, int totalNumberOfChunks, String filename)
						
						ControllerChunkServerToReadResponseToClient controllerReponse = new ControllerChunkServerToReadResponseToClient(shardNumberWithChunkServerSize, shardsWithChunkServerMap, chunkNumber, totalNumberOfChunks, filename);
						
						System.out.println("About to send a Client Read Request (Read Request type: " + controllerReponse.getType()  + ") to: " + clientNode.getNodeIPAddress());
						if (clientNodesMap.containsKey(clientNode)) {
							TCPSender sender = clientNodesMap.get(clientNode);

							System.out.println("Sending now.");
							sender.sendData(controllerReponse.getBytes());
						} else {
							System.out.println("Can't find Client node to send to.");
						}
					} else {
						System.out.println("No ChunkServers available to read the file from.");
					}
				} else {
					System.out.println("File is not stored on any server.");
				}
			}
		} catch  (IOException ioe) {
			ioe.printStackTrace();
		} 
			
		if (DEBUG) { System.out.println("end Controller handleClientReadRequest"); }
	}
	
}
