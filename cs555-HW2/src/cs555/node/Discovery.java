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
import cs555.wireformats.DiscoveryRegisterResponseToPeer;
import cs555.wireformats.DiscoverySendRandomNodeToPeer;
import cs555.wireformats.Event;
import cs555.wireformats.PeerRegisterRequestToDiscovery;
import cs555.wireformats.Protocol;

public class Discovery implements Node {

	private static final boolean DEBUG = false;
	private int portNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private LinkedList<PeerRegisterRequestToDiscovery> registrationLinkedList;
	private HashMap<NodeInformation, TCPSender> peerNodesMap;
	private static Discovery discovery;
	private final static Logger LOGGER = Logger.getLogger(Discovery.class.getName());
	private HashMap<String, TableEntry> nodeTableEntryMap;
	
	private Discovery(int portNumber) {
		this.portNumber = portNumber;
		this.registrationLinkedList = new LinkedList<PeerRegisterRequestToDiscovery>();
		this.peerNodesMap = new HashMap<NodeInformation, TCPSender>();
		
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
		
		
		String IP = peerRegisterRequest.getTableEntry().getNodeInformation().getNodeIPAddress();
		int port = peerRegisterRequest.getTableEntry().getNodeInformation().getNodePortNumber();
		
		if (DEBUG) { System.out.println("Discovery received a message type: " + peerRegisterRequest.getType()); }
		
		System.out.println("Discovery received a PeerRegisterRequestToDiscovery from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		NodeInformation ni = new NodeInformation(IP, port);

		// need to now perform some functionality to add a new peer node to the table carefully
		registerNewNode(peerRegisterRequest.getTableEntry());

		if (DEBUG) { System.out.println("end Discovery handlePeerRegisterRequestToDiscovery"); }
	}
	
	
	
	private void registerNewNode(TableEntry te) {
		byte status = 0;
		String message = "";
		
		synchronized (nodeTableEntryMap) {
			if (!nodeTableEntryMap.containsKey(te.getIdentifier())) {
				String randomNode = getRandomNode();
				if (randomNode != null) {
					// need to tell the random node to insert this node into the system
					// send a message to the new node to insert the node at this random node and update the table and leaf set
					try {
						TableEntry registeredEntry = nodeTableEntryMap.get(randomNode);
						
						DiscoverySendRandomNodeToPeer sendRandomNode = new DiscoverySendRandomNodeToPeer(registeredEntry);
						
						Socket socket = new Socket(te.getNodeInformation().getNodeIPAddress(), te.getNodeInformation().getNodePortNumber());
						TCPSender sender = new TCPSender(socket);
						
						sender.sendData(sendRandomNode.getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				} else {
					// could be empty right now, simply add the first one then as the first node
					nodeTableEntryMap.put(te.getIdentifier(), te);

					try {
						Socket socket = new Socket(te.getNodeInformation().getNodeIPAddress(), te.getNodeInformation().getNodePortNumber());
						TCPSender sender;
						
						sender = new TCPSender(socket);
						peerNodesMap.put(te.getNodeInformation(), sender);
						
						// send message that peer node is successfully registered
						status = 1;
						message = "Peer Node is the first Registered";
						DiscoveryRegisterResponseToPeer discoveryResponse = new DiscoveryRegisterResponseToPeer(status, message);
						
						sender.sendData(discoveryResponse.getBytes());
						
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					
				}
			} else {
				System.out.println("Node Identifer already exists in the map.");
				// send a rejection to the peer so that it registers again
				status = 2;
				message = "Node Identifer already exists in the map";
				DiscoveryRegisterResponseToPeer discoveryResponse = new DiscoveryRegisterResponseToPeer(status, message);
				
				try {
					Socket socket = new Socket(te.getNodeInformation().getNodeIPAddress(), te.getNodeInformation().getNodePortNumber());
					TCPSender sender = new TCPSender(socket);
					
					sender.sendData(discoveryResponse.getBytes());
					
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
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

	private void listNodes() {
		
	}
	
}
