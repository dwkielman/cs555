package cs555.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.NodeInformation;
import cs555.util.TableEntry;
import cs555.wireformats.Event;
import cs555.wireformats.PeerRegisterRequestToDiscovery;
import cs555.wireformats.Protocol;

public class Discovery implements Node {

	private static final boolean DEBUG = false;
	private int portNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private LinkedList<PeerRegisterRequestToDiscovery> registrationLinkedList;
	private static Discovery discovery;
	private final static Logger LOGGER = Logger.getLogger(Discovery.class.getName());
	private HashMap<String, TableEntry> nodeTableEntryMap;
	
	private Discovery(int portNumber) {
		this.portNumber = portNumber;
		this.registrationLinkedList = new LinkedList<PeerRegisterRequestToDiscovery>();

		try {
			TCPServerThread controllerServerThread = new TCPServerThread(this.portNumber, this);
			this.tCPServerThread = controllerServerThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			System.out.println("Discovery TCPServerThread running.");
			
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, null, e);
			e.printStackTrace();
		}
	}
	
	public Discovery() {}
	
	public static void main(String[] args) {
		
		// requires 3 argument to initialize a peer
		if(args.length != 1) {
			System.out.println("Invalid Arguments. Must include a port number.");
            return;
        }
		
		int discoveryPortNumber = 0;
		
		try {
			discoveryPortNumber = Integer.parseInt(args[0]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Argument must be a number.");
			nfe.printStackTrace();
		}
		
		String discoveryIP = "";
		
        try{
        	discoveryIP = InetAddress.getLocalHost().getHostAddress();
        	System.out.println("Trying to get discoveryIP");
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }
        
        System.out.println("Controller is running at IP Address: " + discoveryIP + " on Port Number: " + discoveryPortNumber);
        discovery = new Discovery(discoveryPortNumber);
        discovery.handleUserInput();
	}
	
	@Override
	public synchronized void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event Type " + eventType + " passed to Controller."); }
		switch(eventType) {
			// PEER_REGISTER_REQUEST_TO_DISCOVERY = 7000
			case Protocol.PEER_REGISTER_REQUEST_TO_DISCOVERY:
				handlePeerRegisterRequestToDiscovery(event);
				break;
				/**
			// CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER = 7003
			case Protocol.CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER:
				handleChunkServerSendMajorHeartbeatToController(event);
				break;
			// CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER = 7004
			case Protocol.CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER:
				handleChunkServerSendMinorHeartbeatToController(event);
				break;
			// CHUNKSERVER_SEND_CORRUPT_CHUNK_T0_CONTROLLER = 7005
			case Protocol.CHUNKSERVER_SEND_CORRUPT_CHUNK_T0_CONTROLLER:
				handleChunkServerSendCorruptChunkToController(event);
				break;
			// CHUNKSERVER_DELETED_CHUNK_TO_CONTROLLER = 7007
			case Protocol.CHUNKSERVER_DELETED_CHUNK_TO_CONTROLLER:
				handleChunkServerDeletedChunkToController(event);
				break;
			// CHUNKSERVER_NOTIFY_FIX_SUCCESS_TO_CONTROLLER = 7008
			case Protocol.CHUNKSERVER_NOTIFY_FIX_SUCCESS_TO_CONTROLLER:
				handleChunkServerNotifyFixSuccessToController(event);
				break;
			// CHUNKSERVER_SEND_ONLY_CORRUPT_CHUNK_T0_CONTROLLER = 7009
			case Protocol.CHUNKSERVER_SEND_ONLY_CORRUPT_CHUNK_T0_CONTROLLER:
				handleChunkServerSendOnlyCorruptChunkToController(event);
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
				**/
			default:
				System.out.println("Invalid Event to Node.");
				return;
		}
	}

	@Override
	public void setLocalHostPortNumber(int localPort) {
		this.portNumber = localPort;
		
	}
	
	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Ready for input.");
			
        while(true) {
            System.out.println("Options:\n[list-nodes] List Node in the system\n[Q] Quit\nPlease type your request: ");
            String input = scan.nextLine();
            
            input = input.toUpperCase();
            switch (input) {
            	case "list-nodes":
            		if (DEBUG) { System.out.println("User selected list-nodes"); }
            		listNodes();
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
	
	private void handlePeerRegisterRequestToDiscovery(Event event) {
		if (DEBUG) { System.out.println("begin Discovery PeerRegisterRequestToDiscovery"); }
		PeerRegisterRequestToDiscovery peerRegisterRequest = (PeerRegisterRequestToDiscovery) event;
		String IP = peerRegisterRequest.getIPAddress();
		int port = peerRegisterRequest.getPortNumber();
		
		if (DEBUG) { System.out.println("Discovery received a message type: " + peerRegisterRequest.getType()); }
		
		System.out.println("Discovery received a PeerRegisterRequestToDiscovery from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);

		// need to now perform some functionality to strategically add a new peer node to the table
		
		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the map of nodes
			/**
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
			
			DiscoveryRegisterResponseToPeer discoveryRegisterResponse = new DiscoveryRegisterResponseToPeer(status, message);
			sender.sendData(discoveryRegisterResponse.getBytes());
			**/
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if (DEBUG) { System.out.println("end Discovery handlePeerRegisterRequestToDiscovery"); }
	}
	
	
	private void registerNewNode(TableEntry te) {
		
		synchronized (nodeTableEntryMap) {
			if (!nodeTableEntryMap.containsKey(te.getIdentifier())) {
				String randomNode = getRandomNode();
				if (randomNode != null) {
					// need to tell the random node to insert this node into the system
					
				} else {
					System.out.println("ERROR: Getting node in system did not work.");
				}
			} else {
				System.out.println("Node already exists in the map.");
			}
		}
	}
	
	private String getRandomNode() {
		String randomNodeIdentifier = null;
		List<String> keysAsArray;
		synchronized (nodeTableEntryMap) {
			keysAsArray = new ArrayList<String>(nodeTableEntryMap.keySet());
		}
		
		if (!keysAsArray.isEmpty()) {
			Random random = new Random();
			randomNodeIdentifier = keysAsArray.get(random.nextInt(keysAsArray.size()));
		}
		
		return randomNodeIdentifier;
	}
	
	private void detectCollisions() {
		
	}
	
	private void listNodes() {
		
	}
	
}
