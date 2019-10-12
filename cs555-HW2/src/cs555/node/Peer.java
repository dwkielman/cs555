package cs555.node;

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
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import cs555.transport.TCPReceiverThread;
import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.Direction;
import cs555.util.NodeInformation;
import cs555.util.RoutingTable;
import cs555.util.TableEntry;
import cs555.wireformats.DiscoveryRegisterResponseToPeer;
import cs555.wireformats.DiscoverySendRandomNodeToPeer;
import cs555.wireformats.Event;
import cs555.wireformats.PeerForwardFileToPeer;
import cs555.wireformats.PeerForwardJoinRequestToPeer;
import cs555.wireformats.PeerForwardReadRequestToPeer;
import cs555.wireformats.PeerForwardStoreRequestToPeer;
import cs555.wireformats.PeerJoinRequestFoundDestinationToPeer;
import cs555.wireformats.PeerJoinRequestToPeer;
import cs555.wireformats.PeerLeftLeafRouteTraceToPeer;
import cs555.wireformats.PeerLeftLeafUpdateCompleteToPeer;
import cs555.wireformats.PeerRegisterRequestToDiscovery;
import cs555.wireformats.PeerRemoveFromRoutingTableToPeer;
import cs555.wireformats.PeerRemovePeerToDiscovery;
import cs555.wireformats.PeerRemoveToLeftLeafPeer;
import cs555.wireformats.PeerRemoveToRightLeafPeer;
import cs555.wireformats.PeerRightLeafRouteTraceToPeer;
import cs555.wireformats.PeerRightLeafUpdateCompleteToPeer;
import cs555.wireformats.PeerSendFileToStoreData;
import cs555.wireformats.PeerStoreDestinationToStoreData;
import cs555.wireformats.PeerUpdateCompleteAndInitializedToDiscovery;
import cs555.wireformats.PeerUpdateLeftLeafPeerNode;
import cs555.wireformats.PeerUpdateRightLeafPeerNode;
import cs555.wireformats.PeerUpdateRoutingTableToPeerNode;
import cs555.wireformats.Protocol;
import cs555.wireformats.StoreDataSendFileToPeer;
import cs555.wireformats.StoreDataSendReadRequestToPeer;
import cs555.wireformats.StoreDataSendStoreRequestToPeer;

public class Peer implements Node {

	private static boolean DEBUG = false;
	private NodeInformation discoveryNodeInformation;
	private String localHostIPAddress;
	private String peerNodeIdentifier;
	private int localHostPortNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender discoverySender;
	private TCPReceiverThread peerReceiverThread;
	private NodeInformation peerNodeInformation;
	private AtomicBoolean initialized;
	private AtomicBoolean leftLeafComplete;
	private AtomicBoolean rightLeafComplete;
	private TableEntry peerTableEntry;
	private TableEntry leftLeafTableEntry;
	private TableEntry rightLeafTableEntry;
	private RoutingTable routingTable;
	private String storedFileLocation;
	private HashMap<String, String> filesWithKeyMap;
	private static Peer peer;
	private final static Logger LOGGER = Logger.getLogger(Discovery.class.getName());
	
	private static final int SIZE_OF_CHUNK = 1024 * 64;
	
	private Peer(String controllerIPAddress, int controllerPortNumber, int peerPortNumber, String peerIdentifier) {
		this.discoveryNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.peerNodeIdentifier = peerIdentifier;
		this.initialized = new AtomicBoolean(false);
		this.leftLeafComplete = new AtomicBoolean(false);
		this.rightLeafComplete = new AtomicBoolean(false);
		this.leftLeafTableEntry = null;
		this.rightLeafTableEntry = null;
		this.routingTable = new RoutingTable(this.peerNodeIdentifier);
		this.routingTable.generateRoutingTable();
		this.filesWithKeyMap = new HashMap<String, String>();
		
		try {
			TCPServerThread serverThread = new TCPServerThread(peerPortNumber, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			
			this.peerNodeInformation = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
			
			this.peerTableEntry = new TableEntry(peerNodeIdentifier, peerNodeInformation, localHostIPAddress);

			
			this.storedFileLocation = "/tmp/stored_dkielman/";
			//System.out.println("Data will be stored at: " + this.storedFileLocation);
			LOGGER.info("Data will be stored at: " + this.storedFileLocation);
			
			//System.out.println("Peer Is Running and beginning initialization with following details: Identifier: " + this.peerTableEntry.getIdentifier() + " : Nickname: " + this.peerTableEntry.getNickname() + " : NodeInformation: " + this.peerTableEntry.getNodeInformation());
			LOGGER.info("Peer Is Running and beginning initialization with following details: Identifier: " + this.peerTableEntry.getIdentifier() + " : Nickname: " + this.peerTableEntry.getNickname() + " : NodeInformation: " + this.peerTableEntry.getNodeInformation());
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		
		// Once the initialization is complete, client should send a registration request to the controller.
		connectToDisocvery();
	}
	
	public static void main(String[] args) {
			
		String discoveryIPAddress = args[0];
		int discoveryPortNumber = 0;
		int peerPortNumber = 0;
		String peerIdentifier = null;
		
		// requires 3 argument to initialize a peer, auto-generate identifier in this case
		if (args.length == 3) {
			peerIdentifier = generateIdentifier();
		} else if (args.length == 4) {
			peerIdentifier = args[3];
        } else {
        	//System.out.println("Invalid Arguments. Must include a Discovery IP Address, Discovery Port Number, Peer's Port Number and an optional identifier.");
        	LOGGER.severe("Invalid Arguments. Must include a Discovery IP Address, Discovery Port Number, Peer's Port Number and an optional identifier.");
            return;
        }
		
		try {
			discoveryPortNumber = Integer.parseInt(args[1]);
			peerPortNumber = Integer.parseInt(args[2]);
		} catch (NumberFormatException nfe) {
			//System.out.println("Invalid argument. Arguments must be a number.");
			LOGGER.severe("Invalid argument. Arguments must be a number.");
			nfe.printStackTrace();
		}
		peer = new Peer(discoveryIPAddress, discoveryPortNumber, peerPortNumber, peerIdentifier);
		
	}
	
	private static String generateIdentifier() {
		Long timeLong = System.nanoTime();
        String timeHexString = Long.toHexString(timeLong);
        String randomIdentifier = timeHexString.substring(timeHexString.length() - 4);
        randomIdentifier = randomIdentifier.toUpperCase();
        return randomIdentifier;
	}

	@Override
	public void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event " + eventType + " Passed to Peer."); }
		switch(eventType) {
			// DISCOVERY_REGISTER_RESPONSE_TO_PEER = 6000
			case Protocol.DISCOVERY_REGISTER_RESPONSE_TO_PEER:
				handleDiscvoeryRegisterResponse(event);
				break;
			// DISCOVERY_SEND_RANDOM_NODE_TO_PEER = 6001
			case Protocol.DISCOVERY_SEND_RANDOM_NODE_TO_PEER:
				handleDiscoverySendRandomNodeToPeer(event);
				break;
			// PEER_JOIN_REQUEST_TO_PEER = 7001
			case Protocol.PEER_JOIN_REQUEST_TO_PEER:
				handlePeerJoinRequestToPeer(event);
				break;
			// PEER_FORWARD_JOIN_REQUEST_TO_PEER = 7002
			case Protocol.PEER_FORWARD_JOIN_REQUEST_TO_PEER:
				handlePeerForwardJoinRequestToPeer(event);
				break;
			// PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER = 7003
			case Protocol.PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER:
				handlePeerJoinRequestFoundDestinationToPeer(event);
				break;
			// PEER_UPDATE_LEFT_LEAF_PEER = 7004
			case Protocol.PEER_UPDATE_LEFT_LEAF_PEER:
				handlePeerUpdateLeftLeafPeerNode(event);
				break;
			// PEER_UPDATE_RIGHT_LEAF_PEER = 7005
			case Protocol.PEER_UPDATE_RIGHT_LEAF_PEER:
				handlePeerUpdateRightLeafPeerNode(event);
				break;
			// PEER_UPDATE_ROUTING_TABLE_TO_PEER = 7006
			case Protocol.PEER_UPDATE_ROUTING_TABLE_TO_PEER:
				handlePeerUpdateRoutingTableToPeerNode(event);
				break;
			// PEER_FORWARD_STORE_REQUEST_TO_PEER = 7008
			case Protocol.PEER_FORWARD_STORE_REQUEST_TO_PEER:
				handlePeerForwardStoreRequestToPeer(event);
				break;
			//PEER_FORWARD_READ_REQUEST_TO_PEER = 7010
			case Protocol.PEER_FORWARD_READ_REQUEST_TO_PEER:
				handlePeerForwardReadRequestToPeer(event);
				break;
			// PEER_FORWARD_FILE_TO_PEER = 7011
			case Protocol.PEER_FORWARD_FILE_TO_PEER:
				handlePeerForwardFileToPeer(event);
				break;
			// PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER = 7012
			case Protocol.PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER:
				handlePeerLeftLeafUpdateCompleteToPeer(event);
				break;
			// PEER_RIGHT_LEAF_UPDATE_COMPLETE_TO_PEER = 7013
			case Protocol.PEER_RIGHT_LEAF_UPDATE_COMPLETE_TO_PEER:
				handlePeerRightLeafUpdateCompleteToPeer(event);
				break;
			// PEER_REMOVE_TO_LEFT_LEAF_PEER = 7016
			case Protocol.PEER_REMOVE_TO_LEFT_LEAF_PEER:
				handlePeerRemoveToLeftLeafPeer(event);
				break;
			// PEER_REMOVE_TO_RIGHT_LEAF_PEER = 7017
			case Protocol.PEER_REMOVE_TO_RIGHT_LEAF_PEER:
				handlePeerRemoveToRightLeafPeer(event);
				break;
			// PEER_REMOVE_FROM_ROUTING_TABLE_TO_PEER = 7018
			case Protocol.PEER_REMOVE_FROM_ROUTING_TABLE_TO_PEER:
				handlePeerRemoveFromRoutingTableToPeer(event);
				break;
			// PEER_LEFT_LEAF_ROUTE_TRACE_TO_PEER = 7019
			case Protocol.PEER_LEFT_LEAF_ROUTE_TRACE_TO_PEER:
				handlePeerLeftLeafRouteTraceToPeer(event);
				break;
			// PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER = 7020
			case Protocol.PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER:
				handlePeerRightLeafRouteTraceToPeer(event);
				break;
			// STOREDATA_SEND_STORE_REQUEST_TO_PEER = 8001
			case Protocol.STOREDATA_SEND_STORE_REQUEST_TO_PEER:
				handleStoreDataSendStoreRequestToPeer(event);
				break;
			// STOREDATA_SEND_FILE_TO_PEER = 8002
			case Protocol.STOREDATA_SEND_FILE_TO_PEER:
				handleStoreDataSendFileToPeer(event);
				break;
			// STOREDATA_SEND_READ_REQUEST_TO_PEER = 8004
			case Protocol.STOREDATA_SEND_READ_REQUEST_TO_PEER:
				handleStoreDataSendReadRequestToPeer(event);
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
	
	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Ready for input.");
			
        while(true) {
            System.out.println("Options:\n[remove] Gracefully remove the node from the DHT\n[print-routing-table] Print the Routing Table"
            		+ "\n[print-leaf-set] Print the Leaf Set\n[print-files-list] Print the list of Files that are currently stored on this peer\n[Q] Quit\nPlease type your request: ");
            String input = scan.nextLine();
            
            //input = input.toUpperCase();
            switch (input) {
            	case "remove":
            		if (DEBUG) { System.out.println("User selected remove"); }
            		removePeer();
            		break;
            	case "print-routing-table":
            		if (DEBUG) { System.out.println("User selected print-routing-table"); }
            		printRoutingTable();
            		break;
            	case "print-leaf-set":
            		if (DEBUG) { System.out.println("User selected print-leaf-set"); }
            		printLeafSet();
            		break;
            	case "print-files-list":
            		if (DEBUG) { System.out.println("User selected print-files-list"); }
            		printFilesList();
            		break;
            	case "left-leaf-route-trace":
            		if (DEBUG) { System.out.println("User selected left-leaf-route-trace"); }
            		sendPeerLeftLeafRouteTraceToPeer();
            		break;
            	case "right-leaf-route-trace":
            		if (DEBUG) { System.out.println("User selected right-leaf-route-trace"); }
            		sendPeerRightLeafRouteTraceToPeer();
            		break;
            	case "Q":
            		if (DEBUG) { System.out.println("User selected Quit."); }
            		System.out.println("Quitting program. Goodbye.");
            		System.exit(1);
            	case "q":
            		if (DEBUG) { System.out.println("User selected Quit."); }
            		System.out.println("Quitting program. Goodbye.");
            		System.exit(1);
            	default:
            		System.out.println("Command unrecognized. Please enter a valid input.");
            }
        }
	}
	
	private void connectToDisocvery() {
		if (DEBUG) { System.out.println("begin Peer connectToDisocvery"); }
		try {
			System.out.println("Attempting to connect to Discovery " + this.discoveryNodeInformation.getNodeIPAddress() + " at Port Number: " + this.discoveryNodeInformation.getNodePortNumber());
			Socket discoverySocket = new Socket(this.discoveryNodeInformation.getNodeIPAddress(), this.discoveryNodeInformation.getNodePortNumber());
			
			System.out.println("Starting TCPReceiverThread with Discovery");
			peerReceiverThread = new TCPReceiverThread(discoverySocket, this);
			Thread tcpReceiverThread = new Thread(this.peerReceiverThread);
			tcpReceiverThread.start();
			
			System.out.println("TCPReceiverThread with Discovery started");
			System.out.println("Sending to " + this.discoveryNodeInformation.getNodeIPAddress() + " on Port " +  this.discoveryNodeInformation.getNodePortNumber());
			
			this.discoverySender = new TCPSender(discoverySocket);

			PeerRegisterRequestToDiscovery peerRegisterRequest = new PeerRegisterRequestToDiscovery(this.peerTableEntry);

			if (DEBUG) { System.out.println("Peer about to send message type: " + peerRegisterRequest.getType()); }
			
			this.discoverySender.sendData(peerRegisterRequest.getBytes());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		if (DEBUG) { System.out.println("end Peer connectToDisocvery"); }
	}
	
	private void handleDiscvoeryRegisterResponse(Event event) {
		if (DEBUG) { System.out.println("begin Peer handleDiscvoeryRegisterResponse"); }
		DiscoveryRegisterResponseToPeer peerRegisterResponse = (DiscoveryRegisterResponseToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + peerRegisterResponse.getType()); }
		
		// successful registration
		if (peerRegisterResponse.getStatusCode() == (byte) 1) {
			//this.initialized.getAndSet(true);
			System.out.println("Registration Request Succeeded.");
			System.out.println(String.format("Message: %s", peerRegisterResponse.getAdditionalInfo()));
			
			bothLeafUpdatesComplete();
			//handleUserInput();
		// unsuccessful registration due to conflict with duplicate identifier
		} else if (peerRegisterResponse.getStatusCode() == (byte) 2) {
			System.out.println("Registration Request Failed due to duplicate identifier in system. Generating new one and attempting registration.");
			String newIdentifier = generateIdentifier();
			synchronized (peerNodeIdentifier) {
				peerNodeIdentifier = newIdentifier;
			}
			synchronized (peerTableEntry) {
				peerTableEntry.setIdentifier(newIdentifier);
			}
			
			// attempt to register again with new identifier
			PeerRegisterRequestToDiscovery peerRegisterRequest = new PeerRegisterRequestToDiscovery(this.peerTableEntry);
			
			try {
				this.discoverySender.sendData(peerRegisterRequest.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		// unsuccessful registration
		} else {
			System.out.println("Registration Request Failed. Exiting.");
            System.out.println(String.format("Message: %s", peerRegisterResponse.getAdditionalInfo()));
            System.exit(0);
		}
		if (DEBUG) { System.out.println("end Peer handleDiscvoeryRegisterResponse"); }
	}
	
	// sent a random node that we will request to join with
	private void handleDiscoverySendRandomNodeToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handleDiscoverySendRandomNodeToPeer"); }
		DiscoverySendRandomNodeToPeer randomPeerResponse = (DiscoverySendRandomNodeToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + randomPeerResponse.getType()); }
		
		// tableEntry we will contact to join with
		TableEntry registeredTableEntry = randomPeerResponse.getTableEntry();
		
		ArrayList<String> traceList = new ArrayList<String>();
		traceList.add(registeredTableEntry.getIdentifier());
		
		System.out.println("[INFO] Random PEER : " + registeredTableEntry);
		
		PeerJoinRequestToPeer joinRequest = new PeerJoinRequestToPeer(this.peerTableEntry, traceList.size(), traceList, 0);
		
		try {
			Socket socket = new Socket(registeredTableEntry.getNodeInformation().getNodeIPAddress(), registeredTableEntry.getNodeInformation().getNodePortNumber());
			TCPSender sender = new TCPSender(socket);
			sender.sendData(joinRequest.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("end Peer handleDiscoverySendRandomNodeToPeer"); }
	}
	
	// new peer is attempting to join so need to route to the correct node for this to connect to
	private void handlePeerJoinRequestToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerJoinRequestToPeer"); }
		PeerJoinRequestToPeer joinRequest = (PeerJoinRequestToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + joinRequest.getType()); }
		
		ArrayList<String> traceList = joinRequest.getTraceList();
		
		TableEntry joiningTableEntry = joinRequest.getTableEntry();
		
		String newIdentifier = joiningTableEntry.getIdentifier();
		
		RoutingTable newIdentifierRoutingTable = new RoutingTable(newIdentifier);
		newIdentifierRoutingTable.generateRoutingTable();
		
		System.out.println("");
        System.out.println("[INFO] Join request from PEER : " + newIdentifier);
		
		int hopCount = joinRequest.getHopCount();
		hopCount++;
		
		// need to update the joining peer's routing table with information about all of the entries currently in the routing table
		generateEntriesInRoutingTable(newIdentifier, newIdentifierRoutingTable);
		
		// need to find what the next peer to send the joining peer to
		TableEntry nextPeer = lookup(newIdentifier);
		
		// no other entries yet, the identifier is itself
		if (nextPeer.getIdentifier().equalsIgnoreCase(newIdentifier)) {
			int index = getIndexOfPrefixNotMatching(nextPeer.getIdentifier(), this.peerNodeIdentifier);
			String lookupEntryString = nextPeer.getIdentifier().substring(0, index + 1);
			
			synchronized (this.routingTable) {
				HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
				TableEntry te = row.get(lookupEntryString);
				if (te.getIdentifier().equals(nextPeer.getIdentifier())) {
					row.put(lookupEntryString, null);
				}
			}
			nextPeer = lookup(newIdentifier);
		}
		
		if (nextPeer.getNodeInformation().equals(joiningTableEntry.getNodeInformation())) {
			int index = getIndexOfPrefixNotMatching(nextPeer.getIdentifier(), this.peerNodeIdentifier);
			String lookupEntryString = nextPeer.getIdentifier().substring(0, index + 1);
			
			synchronized (this.routingTable) {
				HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
				TableEntry te = row.get(lookupEntryString);
				if (te.getIdentifier().equals(nextPeer.getIdentifier())) {
					row.put(lookupEntryString, null);
				}
			}
			nextPeer = lookup(newIdentifier);
		}
		
		// this node is the final destination for the joining peer
		if (nextPeer.getIdentifier().equals(this.peerNodeIdentifier)) {
			System.out.println("[INFO] Destination Found PEER : " + this.peerNodeIdentifier);
			TableEntry leftLeafEntry = null;
			TableEntry rightLeafEntry = null;

			if (this.leftLeafTableEntry == null) {
				//synchronized (this.leftLeafTableEntry) {
				leftLeafEntry = this.peerTableEntry;
				//}
			} else {
				synchronized (this.leftLeafTableEntry) {
					leftLeafEntry = this.leftLeafTableEntry;
				}
			}
			
			
			if (this.rightLeafTableEntry == null) {
				//synchronized (this.rightLeafTableEntry) {
				rightLeafEntry = this.peerTableEntry;
				//}
			} else {
				synchronized (this.rightLeafTableEntry) {
					rightLeafEntry = this.rightLeafTableEntry;
				}
			}
			
			PeerJoinRequestFoundDestinationToPeer foundDestination = new PeerJoinRequestFoundDestinationToPeer(this.peerTableEntry, leftLeafEntry, rightLeafEntry, newIdentifierRoutingTable, traceList.size(), traceList, hopCount);
			
			try {
				Socket socket = new Socket(joiningTableEntry.getNodeInformation().getNodeIPAddress(), joiningTableEntry.getNodeInformation().getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				sender.sendData(foundDestination.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		// forward the joining peer to the next node to find its placement
		} else {
			System.out.println("[INFO] Next PEER : " + nextPeer.getIdentifier());
			traceList.add(nextPeer.getIdentifier());
			
			PeerForwardJoinRequestToPeer forwardRequest = new PeerForwardJoinRequestToPeer(joiningTableEntry, newIdentifierRoutingTable, traceList.size(), traceList, hopCount);
			
			try {
				Socket socket = new Socket(nextPeer.getNodeInformation().getNodeIPAddress(), nextPeer.getNodeInformation().getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				sender.sendData(forwardRequest.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerJoinRequestToPeer"); }
	}
	
	// a lot of the logic here should be similar to above, copy/paste and edit as needed
	private void handlePeerForwardJoinRequestToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerForwardJoinRequestToPeer"); }
		PeerForwardJoinRequestToPeer forwardJoinRequest = (PeerForwardJoinRequestToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + forwardJoinRequest.getType()); }
		
		ArrayList<String> traceList = forwardJoinRequest.getTraceList();
		
		TableEntry joiningTableEntry = forwardJoinRequest.getTableEntry();
		
		String newIdentifier = joiningTableEntry.getIdentifier();
		
		RoutingTable newIdentifierRoutingTable = forwardJoinRequest.getRoutingTable();
		
		System.out.println("");
        System.out.println("[INFO] Join request from PEER : " + newIdentifier);
		
		int hopCount = forwardJoinRequest.getHopCount();
		hopCount++;
		
		// need to update the joining peer's routing table with information about all of the entries currently in the routing table
		generateEntriesInRoutingTable(newIdentifier, newIdentifierRoutingTable);
		
		// need to find what the next peer to send the joining peer to
		TableEntry nextPeer = lookup(newIdentifier);
		
		// no other entries yet, the identifier is itself
		if (nextPeer.getIdentifier().equalsIgnoreCase(newIdentifier)) {
			int index = getIndexOfPrefixNotMatching(nextPeer.getIdentifier(), this.peerNodeIdentifier);
			String lookupEntryString = nextPeer.getIdentifier().substring(0, index + 1);
			
			synchronized (this.routingTable) {
				HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
				TableEntry te = row.get(lookupEntryString);
				if (te.getIdentifier().equals(nextPeer.getIdentifier())) {
					row.put(lookupEntryString, null);
				}
			}
			nextPeer = lookup(newIdentifier);
		}
		
		if (nextPeer.getNodeInformation().equals(joiningTableEntry.getNodeInformation())) {
			int index = getIndexOfPrefixNotMatching(nextPeer.getIdentifier(), this.peerNodeIdentifier);
			String lookupEntryString = nextPeer.getIdentifier().substring(0, index + 1);
			
			synchronized (this.routingTable) {
				HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
				TableEntry te = row.get(lookupEntryString);
				if (te.getIdentifier().equals(nextPeer.getIdentifier())) {
					row.put(lookupEntryString, null);
				}
			}
			nextPeer = lookup(newIdentifier);
		}
		
		// this node is the final destination for the joining peer
		if (nextPeer.getIdentifier().equals(this.peerNodeIdentifier)) {
			System.out.println("[INFO] Destination Found PEER : " + this.peerNodeIdentifier);
			TableEntry leftLeafEntry = null;
			TableEntry rightLeafEntry = null;
			
			synchronized (this.leftLeafTableEntry) {
				leftLeafEntry = this.leftLeafTableEntry;
			}
			
			synchronized (this.rightLeafTableEntry) {
				rightLeafEntry = this.rightLeafTableEntry;
			}
			
			PeerJoinRequestFoundDestinationToPeer foundDestination = new PeerJoinRequestFoundDestinationToPeer(this.peerTableEntry, leftLeafEntry, rightLeafEntry, newIdentifierRoutingTable, traceList.size(), traceList, hopCount);
			
			try {
				Socket socket = new Socket(joiningTableEntry.getNodeInformation().getNodeIPAddress(), joiningTableEntry.getNodeInformation().getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				sender.sendData(foundDestination.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		// forward the joining peer to the next node to find its placement
		} else {
			System.out.println("[INFO] Next PEER : " + nextPeer.getIdentifier());
			traceList.add(nextPeer.getIdentifier());
			
			PeerForwardJoinRequestToPeer forwardRequest = new PeerForwardJoinRequestToPeer(joiningTableEntry, newIdentifierRoutingTable, traceList.size(), traceList, hopCount);
			
			try {
				Socket socket = new Socket(nextPeer.getNodeInformation().getNodeIPAddress(), nextPeer.getNodeInformation().getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				sender.sendData(forwardRequest.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerForwardJoinRequestToPeer"); }
	}

	private void handlePeerJoinRequestFoundDestinationToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerJoinRequestFoundDestinationToPeer"); }
		PeerJoinRequestFoundDestinationToPeer foundDestinationForJoinRequest = (PeerJoinRequestFoundDestinationToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + foundDestinationForJoinRequest.getType()); }
		
		this.routingTable = foundDestinationForJoinRequest.getRoutingTable();
		
		// need to populate the routing table accordingly
		// this may not be necessary but would need to test and confirm
		synchronized (this.routingTable) {
			for (Entry<Integer, HashMap<String, TableEntry>> entrySet : this.routingTable.getTable().entrySet()) {
                int key = entrySet.getKey();
                HashMap<String, TableEntry> row = entrySet.getValue();
                for (HashMap.Entry<String, TableEntry> rowEntrySet : row.entrySet()) {
                    String identifier = rowEntrySet.getKey();
                    //TableEntry entry = rowEntrySet.getValue();
                    if (identifier.substring(0, key + 1).equalsIgnoreCase(this.peerNodeIdentifier.substring(0, key + 1))) {
                        row.put(identifier, null);
                    }
                }
            }
		}
		
		TableEntry leftLeaf = foundDestinationForJoinRequest.getLeftLeafTableEntry();
		TableEntry rightLeaf = foundDestinationForJoinRequest.getRightLeafTableEntry();
		TableEntry destinationTableEntry = foundDestinationForJoinRequest.getDestinationTableEntry();
		ArrayList<String> traceList = foundDestinationForJoinRequest.getTraceList();
		int hopCount = foundDestinationForJoinRequest.getHopCount();
		hopCount++;
		
		System.out.println("");
        System.out.println("[INFO] Info received from Destination with ID : " + destinationTableEntry.getIdentifier());
        System.out.println("[INFO] HOPCOUNT: " + hopCount);
        
        String traceString = "";
        for (String s : traceList) {
        	traceString = traceString + s + "-";
        }
        
        traceString = traceString.substring(0, traceString.length() - 1);
        
        System.out.println("[INFO] TRACE: " + traceString);
		
        // populate leaf sets with their data
        if (leftLeaf != null && rightLeaf != null && destinationTableEntry != null) {
        	int leftLeafDistanceFromDestination = distanceCounterClockwiseSearch(destinationTableEntry.getIdentifier(), leftLeaf.getIdentifier());
        	int rightLeafDistanceFromDestination = distanceClockwiseSearch(destinationTableEntry.getIdentifier(), rightLeaf.getIdentifier());
        	
        	int thisPeerNodeDistanceFromDestinationToLeft = distanceCounterClockwiseSearch(destinationTableEntry.getIdentifier(), this.peerNodeIdentifier);
        	int thisPeerNodeDistanceFromDestinationToRight = distanceClockwiseSearch(destinationTableEntry.getIdentifier(), this.peerNodeIdentifier);
        	
        	// find which one is closer between the leafs passed and the destination node and set myself in accordingly
        	if (thisPeerNodeDistanceFromDestinationToLeft < leftLeafDistanceFromDestination) {
        		this.leftLeafTableEntry = leftLeaf;
        		this.rightLeafTableEntry = destinationTableEntry;
        	} else if (thisPeerNodeDistanceFromDestinationToRight < rightLeafDistanceFromDestination) {
        		this.leftLeafTableEntry = destinationTableEntry;
        		this.rightLeafTableEntry = rightLeaf;
        	}
    	// left leaf and right leaf are empty, no other peers in the system yet
        } else if (destinationTableEntry != null) {
        	this.leftLeafTableEntry = destinationTableEntry;
        	this.rightLeafTableEntry = destinationTableEntry;
        }
        
        // need to notify the leaves that this node is now in the system as well as all of the nodes in the routing table
        // update the left leaf
        PeerUpdateLeftLeafPeerNode updateLeftLeaf = new PeerUpdateLeftLeafPeerNode(this.peerTableEntry);
        synchronized (this.leftLeafTableEntry) {
        	try {
				Socket socket = new Socket(this.leftLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.leftLeafTableEntry.getNodeInformation().getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(updateLeftLeaf.getBytes());
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        
        // update the right leaf
        PeerUpdateRightLeafPeerNode updateRightLeaf = new PeerUpdateRightLeafPeerNode(this.peerTableEntry);
        synchronized (this.rightLeafTableEntry) {
        	try {
				Socket socket = new Socket(this.rightLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.rightLeafTableEntry.getNodeInformation().getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(updateRightLeaf.getBytes());
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        
        // update the routing table entries that are registered
        PeerUpdateRoutingTableToPeerNode updateRoutingTable = new PeerUpdateRoutingTableToPeerNode(this.peerTableEntry);
        
        synchronized (this.routingTable) {
			for (Entry<Integer, HashMap<String, TableEntry>> entrySet : this.routingTable.getTable().entrySet()) {
                //int key = entrySet.getKey();
                HashMap<String, TableEntry> row = entrySet.getValue();
                for (HashMap.Entry<String, TableEntry> rowEntrySet : row.entrySet()) {
                    String identifier = rowEntrySet.getKey();
                    TableEntry entry = rowEntrySet.getValue();
                    if (entry != null) {
                    	try {
            				Socket socket = new Socket(entry.getNodeInformation().getNodeIPAddress(), entry.getNodeInformation().getNodePortNumber());
            				
            				TCPSender sender = new TCPSender(socket);
            				sender.sendData(updateRoutingTable.getBytes());
            				
            			} catch (UnknownHostException e) {
            				e.printStackTrace();
            			} catch (IOException e) {
            				e.printStackTrace();
            			} catch (Exception e) {
            				row.put(identifier, null);
            			}
                    }
                }
            }
		}
        
        System.out.println("");
        printRoutingTable();
        System.out.println("");
        printLeafSet();
        System.out.println("");
        
		if (DEBUG) { System.out.println("end Peer handlePeerJoinRequestFoundDestinationToPeer"); }
	}
	
	private void handlePeerUpdateLeftLeafPeerNode(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerUpdateLeftLeafPeerNode"); }
		PeerUpdateLeftLeafPeerNode updateLeftLeaf = (PeerUpdateLeftLeafPeerNode) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + updateLeftLeaf.getType()); }
		
		TableEntry newTableEntry = updateLeftLeaf.getTableEntry();
		
		if (this.rightLeafTableEntry == null) {
			this.rightLeafTableEntry = newTableEntry;
		} else {
			synchronized (this.rightLeafTableEntry) {
				this.rightLeafTableEntry = newTableEntry;
			}
		}
		
		// if the new tableEntry is closer than I am, then need to forward my files to it as it will be the destination for where those files should be
		synchronized (this.filesWithKeyMap) {
			for (String filename : filesWithKeyMap.keySet()) {
				String key = filesWithKeyMap.get(filename);
				synchronized (this.rightLeafTableEntry) {
					String closestIdentifier = getClosestPeer(key, this.rightLeafTableEntry.getIdentifier(), this.peerNodeIdentifier);
					if (closestIdentifier.equals(this.rightLeafTableEntry.getIdentifier())) {
						byte[] fileBytes = readAndGetFile(filename, key);
						if (fileBytes != null) {
							try {
								PeerForwardFileToPeer forwardFile = new PeerForwardFileToPeer(filename, key, fileBytes, this.peerTableEntry);
								
								Socket socket = new Socket(this.rightLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.rightLeafTableEntry.getNodeInformation().getNodePortNumber());
								TCPSender sender = new TCPSender(socket);
								sender.sendData(forwardFile.getBytes());
								
								// data has been forwarded to new peer, remove it from our reference
								this.filesWithKeyMap.remove(filename);
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
		
		// send an update that all files that are closer to the new peer have been transferred
		PeerLeftLeafUpdateCompleteToPeer leftLeafUpdateComplete = new PeerLeftLeafUpdateCompleteToPeer();
		try {
			synchronized (this.rightLeafTableEntry) {
				Socket socket = new Socket(this.rightLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.rightLeafTableEntry.getNodeInformation().getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				sender.sendData(leftLeafUpdateComplete.getBytes());
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("[INFO] Leafset updated");
        printLeafSet();
        
        // update routing table
        String newIdentifier = newTableEntry.getIdentifier();
        int index = getIndexOfPrefixNotMatching(this.peerNodeIdentifier, newIdentifier);
        String lookupString = newIdentifier.substring(0, index + 1);
        
        synchronized (this.routingTable) {
        	HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
        	TableEntry previousEntry = row.get(lookupString);
        	if (previousEntry == null) {
        		row.put(lookupString, newTableEntry);
        		System.out.println("[INFO] Routing table updated");
                printRoutingTable();
        	}
        }
		
		if (DEBUG) { System.out.println("end Peer handlePeerUpdateLeftLeafPeerNode"); }
	}
	
	private void handlePeerUpdateRightLeafPeerNode(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerUpdateRightLeafPeerNode"); }
		PeerUpdateRightLeafPeerNode updateRightLeaf = (PeerUpdateRightLeafPeerNode) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + updateRightLeaf.getType()); }
		
		TableEntry newTableEntry = updateRightLeaf.getTableEntry();
		
		if (this.leftLeafTableEntry == null) {
			this.leftLeafTableEntry = newTableEntry;
		} else {
			synchronized (this.leftLeafTableEntry) {
				this.leftLeafTableEntry = newTableEntry;
			}
		}
		
		// if the new tableEntry is closer than I am, then need to forward my files to it as it will be the destination for where those files should be
		synchronized (this.filesWithKeyMap) {
			for (String filename : filesWithKeyMap.keySet()) {
				String key = filesWithKeyMap.get(filename);
				synchronized (this.leftLeafTableEntry) {
					String closestIdentifier = getClosestPeer(key, this.leftLeafTableEntry.getIdentifier(), this.peerNodeIdentifier);
					if (closestIdentifier.equals(this.leftLeafTableEntry.getIdentifier())) {
						byte[] fileBytes = readAndGetFile(filename, key);
						if (fileBytes != null) {
							try {
								PeerForwardFileToPeer forwardFile = new PeerForwardFileToPeer(filename, key, fileBytes, this.peerTableEntry);
								
								Socket socket = new Socket(this.leftLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.leftLeafTableEntry.getNodeInformation().getNodePortNumber());
								TCPSender sender = new TCPSender(socket);
								sender.sendData(forwardFile.getBytes());
								
								// data has been forwarded to new peer, remove it from our reference
								this.filesWithKeyMap.remove(filename);
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
		
		// send an update that all files that are closer to the new peer have been transferred
		PeerRightLeafUpdateCompleteToPeer rightLeafUpdateComplete = new PeerRightLeafUpdateCompleteToPeer();
		try {
			synchronized (this.leftLeafTableEntry) {
				Socket socket = new Socket(this.leftLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.leftLeafTableEntry.getNodeInformation().getNodePortNumber());
				TCPSender sender = new TCPSender(socket);
				sender.sendData(rightLeafUpdateComplete.getBytes());
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("[INFO] Leafset updated");
        printLeafSet();
        
        // update routing table
        String newIdentifier = newTableEntry.getIdentifier();
        int index = getIndexOfPrefixNotMatching(this.peerNodeIdentifier, newIdentifier);
        String lookupString = newIdentifier.substring(0, index + 1);
        
        synchronized (this.routingTable) {
        	HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
        	TableEntry previousEntry = row.get(lookupString);
        	if (previousEntry == null) {
        		row.put(lookupString, newTableEntry);
        		System.out.println("[INFO] Routing table updated");
                printRoutingTable();
        	}
        }
		
		if (DEBUG) { System.out.println("end Peer handlePeerUpdateRightLeafPeerNode"); }
	}
	
	private void handlePeerUpdateRoutingTableToPeerNode(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerUpdateRoutingTableToPeerNode"); }
		PeerUpdateRoutingTableToPeerNode updateRoutingTable = (PeerUpdateRoutingTableToPeerNode) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + updateRoutingTable.getType()); }
		
		TableEntry newTableEntry = updateRoutingTable.getTableEntry();
		String newTableEntryIdentifier = newTableEntry.getIdentifier();
		
		int index = getIndexOfPrefixNotMatching(this.peerNodeIdentifier, newTableEntryIdentifier);
		String identifier = newTableEntryIdentifier.substring(0, index + 1);
		
		synchronized (this.routingTable) {
			HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
			TableEntry currentEntry = row.get(identifier);
			if (currentEntry == null) {
				row.put(identifier, newTableEntry);
			}
		}
		if (DEBUG) { System.out.println("end Peer handlePeerUpdateRoutingTableToPeerNode"); }
	}
	
	private void handlePeerForwardFileToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerForwardFileToPeer"); }
		PeerForwardFileToPeer forwardedFile = (PeerForwardFileToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + forwardedFile.getType()); }
		
		String filename = forwardedFile.getFilename();
		String key = forwardedFile.getKey();
		byte[] fileData = forwardedFile.getFileBytes();
		TableEntry previousTableEntry = forwardedFile.getTableEntry();
		
		saveFile(filename, key, fileData);
		
		System.out.println("[INFO] File forwarded from PEER : " + previousTableEntry.getIdentifier());
		
		if (DEBUG) { System.out.println("end Peer handlePeerForwardFileToPeer"); }
	}
	
	private void handlePeerLeftLeafUpdateCompleteToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerLeftLeafUpdateCompleteToPeer"); }
		PeerLeftLeafUpdateCompleteToPeer leftLeafUpdateComplete = (PeerLeftLeafUpdateCompleteToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + leftLeafUpdateComplete.getType()); }
		
		synchronized (this.leftLeafComplete) {
			this.leftLeafComplete.getAndSet(true);
		}
		
		if (this.leftLeafComplete.get() && this.rightLeafComplete.get()) {
			bothLeafUpdatesComplete();
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerLeftLeafUpdateCompleteToPeer"); }
	}

	private void handlePeerRightLeafUpdateCompleteToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerRightLeafUpdateCompleteToPeer"); }
		PeerRightLeafUpdateCompleteToPeer rightLeafUpdateComplete = (PeerRightLeafUpdateCompleteToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + rightLeafUpdateComplete.getType()); }
		
		synchronized (this.rightLeafComplete) {
			this.rightLeafComplete.getAndSet(true);
		}
		
		if (this.leftLeafComplete.get() && this.rightLeafComplete.get()) {
			bothLeafUpdatesComplete();
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerRightLeafUpdateCompleteToPeer"); }
	}
	
	private void bothLeafUpdatesComplete() {
		synchronized (this.initialized) {
			this.initialized.getAndSet(true);
		}
		
		System.out.println("[INFO] Initialization Complete!\n");
		
		PeerUpdateCompleteAndInitializedToDiscovery updateComplete = new PeerUpdateCompleteAndInitializedToDiscovery(this.peerTableEntry);
		
		try {
			this.discoverySender.sendData(updateComplete.getBytes());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		handleUserInput();
		
	}
	
	private void handleStoreDataSendStoreRequestToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handleStoreDataSendStoreRequestToPeer"); }
		StoreDataSendStoreRequestToPeer storeRequest = (StoreDataSendStoreRequestToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + storeRequest.getType()); }
		
		String filename = storeRequest.getFilename();
		String key = storeRequest.getKey();
		NodeInformation storeData = storeRequest.getStoreData();
		ArrayList<String> traceList = storeRequest.getTraceList();
		int hopCount = storeRequest.getHopCount();
		hopCount++;
		
		traceList.add(this.peerNodeIdentifier);
		
		TableEntry nextTableEntry = lookup(key);
		
		System.out.println("[INFO] Store request recieved from StoreData.");
		
		
		// this is the peer that should store the data
		if (nextTableEntry.getIdentifier().equals(this.peerNodeIdentifier)) {
			sendPeerStoreDestinationToStoreData(filename, key, traceList, hopCount, storeData);
		} else {
			sendPeerForwardStoreRequestToPeer(filename, key, traceList, hopCount, storeData, nextTableEntry);
		}
		
		if (DEBUG) { System.out.println("end Peer handleStoreDataSendStoreRequestToPeer"); }
	}
	
	private void handlePeerForwardStoreRequestToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerForwardStoreRequestToPeer"); }
		PeerForwardStoreRequestToPeer forwardStoreRequest = (PeerForwardStoreRequestToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + forwardStoreRequest.getType()); }
		
		String filename = forwardStoreRequest.getFilename();
		String key = forwardStoreRequest.getKey();
		NodeInformation storeData = forwardStoreRequest.getStoreData();
		ArrayList<String> traceList = forwardStoreRequest.getTraceList();
		int hopCount = forwardStoreRequest.getHopCount();
		hopCount++;
		
		traceList.add(this.peerNodeIdentifier);
		
		TableEntry nextTableEntry = lookup(key);
		
		System.out.println("[INFO] Store request recieved");
		
		// this is the peer that should store the data
		if (nextTableEntry.getIdentifier().equals(this.peerNodeIdentifier)) {
			sendPeerStoreDestinationToStoreData(filename, key, traceList, hopCount, storeData);
		} else {
			sendPeerForwardStoreRequestToPeer(filename, key, traceList, hopCount, storeData, nextTableEntry);
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerForwardStoreRequestToPeer"); }
	}
	
	private void handleStoreDataSendFileToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handleStoreDataSendFileToPeer"); }
		StoreDataSendFileToPeer storeFile = (StoreDataSendFileToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + storeFile.getType()); }
		
		System.out.println("[INFO] StoreData Saving File.");
		
		String filename = storeFile.getFilename();
		String key = storeFile.getKey();
		byte[] fileData = storeFile.getFileBytes();
		
		saveFile(filename, key, fileData);
		
		if (DEBUG) { System.out.println("end Peer handleStoreDataSendFileToPeer"); }
	}
	
	private void sendPeerStoreDestinationToStoreData(String filename, String key, ArrayList<String> traceList, int hopCount, NodeInformation storeDataNodeInformation) {
		System.out.println("[INFO] Store request Destination Found");
		
		PeerStoreDestinationToStoreData storeDestination = new PeerStoreDestinationToStoreData(filename, key, this.peerNodeInformation, traceList.size(), traceList, hopCount);
		
		try {
			Socket socket = new Socket(storeDataNodeInformation.getNodeIPAddress(), storeDataNodeInformation.getNodePortNumber());
			
			TCPSender sender = new TCPSender(socket);
			sender.sendData(storeDestination.getBytes());
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void sendPeerForwardStoreRequestToPeer(String filename, String key, ArrayList<String> traceList, int hopCount, NodeInformation storeDataNodeInformation, TableEntry nextTableEntry) {
		System.out.println("[INFO] Forwarding Store request to Peer: " + nextTableEntry.getIdentifier());
		
		PeerForwardStoreRequestToPeer forwardStoreRequest = new PeerForwardStoreRequestToPeer(filename, key, storeDataNodeInformation, traceList.size(), traceList, hopCount);
		
		try {
			Socket socket = new Socket(nextTableEntry.getNodeInformation().getNodeIPAddress(), nextTableEntry.getNodeInformation().getNodePortNumber());
			
			TCPSender sender = new TCPSender(socket);
			sender.sendData(forwardStoreRequest.getBytes());
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void handleStoreDataSendReadRequestToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handleStoreDataSendReadRequestToPeer"); }
		StoreDataSendReadRequestToPeer readRequest = (StoreDataSendReadRequestToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + readRequest.getType()); }
		
		String filename = readRequest.getFilename();
		String key = readRequest.getKey();
		NodeInformation storeData = readRequest.getStoreData();
		ArrayList<String> traceList = readRequest.getTraceList();
		int hopCount = readRequest.getHopCount();
		hopCount++;
		
		traceList.add(this.peerNodeIdentifier);
		
		TableEntry nextTableEntry = lookup(key);
		
		System.out.println("[INFO] Read request recieved from StoreData.");
		
		// this is the peer that has the file
		if (nextTableEntry.getIdentifier().equals(this.peerNodeIdentifier)) {
			sendPeerSendFileToStoreData(filename, key, traceList, hopCount, storeData);
		} else {
			sendPeerForwardReadRequestToPeer(filename, key, traceList, hopCount, storeData, nextTableEntry);
		}
		
		if (DEBUG) { System.out.println("end Peer handleStoreDataSendReadRequestToPeer"); }
	}
	
	private void sendPeerSendFileToStoreData(String filename, String key, ArrayList<String> traceList, int hopCount, NodeInformation storeDataNodeInformation) {
		System.out.println("[INFO] Read request Destination Found");
		
		byte[] fileBytes = readAndGetFile(filename, key);
		
		if (fileBytes != null) {
			PeerSendFileToStoreData sendFile = new PeerSendFileToStoreData(filename, key, this.peerNodeInformation, traceList.size(), traceList, hopCount, fileBytes);
			
			try {
				Socket socket = new Socket(storeDataNodeInformation.getNodeIPAddress(), storeDataNodeInformation.getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(sendFile.getBytes());
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("ERROR: Returned null bytes after reading.");
		}
		
	}
	
	private void sendPeerForwardReadRequestToPeer(String filename, String key, ArrayList<String> traceList, int hopCount, NodeInformation storeDataNodeInformation, TableEntry nextTableEntry) {
		System.out.println("[INFO] Forwarding Read request to Peer: " + nextTableEntry.getIdentifier());
		
		PeerForwardReadRequestToPeer forwardReadRequest = new PeerForwardReadRequestToPeer(filename, key, storeDataNodeInformation, traceList.size(), traceList, hopCount);
		
		try {
			Socket socket = new Socket(nextTableEntry.getNodeInformation().getNodeIPAddress(), nextTableEntry.getNodeInformation().getNodePortNumber());
			
			TCPSender sender = new TCPSender(socket);
			sender.sendData(forwardReadRequest.getBytes());
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void handlePeerForwardReadRequestToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerForwardReadRequestToPeer"); }
		PeerForwardReadRequestToPeer forwardReadRequest = (PeerForwardReadRequestToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + forwardReadRequest.getType()); }
		
		String filename = forwardReadRequest.getFilename();
		String key = forwardReadRequest.getKey();
		NodeInformation storeData = forwardReadRequest.getStoreData();
		ArrayList<String> traceList = forwardReadRequest.getTraceList();
		int hopCount = forwardReadRequest.getHopCount();
		hopCount++;
		
		traceList.add(this.peerNodeIdentifier);
		
		TableEntry nextTableEntry = lookup(key);
		
		System.out.println("[INFO] Read request recieved");
		
		// this is the peer that has the file
		if (nextTableEntry.getIdentifier().equals(this.peerNodeIdentifier)) {
			sendPeerSendFileToStoreData(filename, key, traceList, hopCount, storeData);
		} else {
			sendPeerForwardReadRequestToPeer(filename, key, traceList, hopCount, storeData, nextTableEntry);
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerForwardReadRequestToPeer"); }
	}
	
	// note that it this is coming from the OUR right peer
	private void handlePeerRemoveToLeftLeafPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerRemoveToLeftLeafPeer"); }
		PeerRemoveToLeftLeafPeer removeToLeft = (PeerRemoveToLeftLeafPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + removeToLeft.getType()); }
		
		TableEntry te = removeToLeft.getTableEntry();
		
		System.out.println("[INFO] Updating Right Leaf to : " + te);
		
		synchronized (this.rightLeafTableEntry) {
			this.rightLeafTableEntry = te;
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerRemoveToLeftLeafPeer"); }
	}
	
	// note that it this is coming from the OUR left peer
	private void handlePeerRemoveToRightLeafPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerRemoveToRightLeafPeer"); }
		PeerRemoveToRightLeafPeer removeToRight = (PeerRemoveToRightLeafPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + removeToRight.getType()); }
		
		TableEntry te = removeToRight.getTableEntry();
		
		System.out.println("[INFO] Updating Left Leaf to : " + te);
		
		synchronized (this.leftLeafTableEntry) {
			this.leftLeafTableEntry = te;
		}
		
		if (DEBUG) { System.out.println("end Peer handlePeerRemoveToRightLeafPeer"); }
	}
	
	private void handlePeerRemoveFromRoutingTableToPeer(Event event) {
		if (DEBUG) { System.out.println("begin Peer handlePeerRemoveFromRoutingTableToPeer"); }
		PeerRemoveFromRoutingTableToPeer removeFromRoutingTable = (PeerRemoveFromRoutingTableToPeer) event;
		if (DEBUG) { System.out.println("Peer Node got a message type: " + removeFromRoutingTable.getType()); }
		
		TableEntry te = removeFromRoutingTable.getTableEntry();
		int index = getIndexOfPrefixNotMatching(te.getIdentifier(), this.peerNodeIdentifier);
		
		String identifier = te.getIdentifier().substring(0, index + 1);
		
		synchronized (this.routingTable) {
			HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
			TableEntry entry = row.get(identifier);
			// if this row has the identifier for the peer that is leaving, then update the routing table to hold null in this value
			if (entry != null) {
				if (entry.getIdentifier().equals(te.getIdentifier())) {
					row.put(identifier, null);
				}
			}
		}
		
		//handleUserInput();
		
		if (DEBUG) { System.out.println("end Peer handlePeerRemoveFromRoutingTableToPeer"); }
	}
	
	private TableEntry lookup(String newIdentifier) {
		TableEntry nextEntry = null;
		
		// if no other entries on one side then this is the next entry
		if (this.leftLeafTableEntry == null || this.rightLeafTableEntry == null) {
			nextEntry = this.peerTableEntry;
		} else {
			int currentDistanceLeft, currentDistanceRight;
			String closestPeer;
			
			// collect our current distance from the node stored in the leafset for this node
			synchronized (this.leftLeafTableEntry) {
				currentDistanceLeft = distanceCounterClockwiseSearch(this.peerNodeIdentifier, this.leftLeafTableEntry.getIdentifier());
			}
			
			synchronized (this.rightLeafTableEntry) {
				currentDistanceRight = distanceClockwiseSearch(this.peerNodeIdentifier, this.rightLeafTableEntry.getIdentifier());
			}
			
			// gather the distance for the joining node from this node
			int joiningDistanceLeft = distanceCounterClockwiseSearch(this.peerNodeIdentifier, newIdentifier);
			int joiningDistanceRight = distanceClockwiseSearch(this.peerNodeIdentifier, newIdentifier);
			
			// joining node is closer than the current node stored in the left leaf set
			if (joiningDistanceLeft < currentDistanceLeft) {
				// find which is the closest node, either myself or the node in my left leaf
				synchronized (this.leftLeafTableEntry) {
					closestPeer = getClosestPeer(newIdentifier, this.peerNodeIdentifier, this.leftLeafTableEntry.getIdentifier());
					
					// if the closest peer is me, set myself as the nextEntry, otherwise is the peer in my left leaf
					if (closestPeer.equals(this.peerNodeIdentifier)) {
						nextEntry = this.peerTableEntry;
					} else {
						nextEntry = this.leftLeafTableEntry;
					}
				}
			// joining node is closer than the current node stored in the right leaf set
			} else if (joiningDistanceRight < currentDistanceRight) {
				// find which is the closest node, either myself or the node in the right leaf
				synchronized (this.rightLeafTableEntry) {
					closestPeer = getClosestPeer(newIdentifier, this.peerNodeIdentifier, this.rightLeafTableEntry.getIdentifier());
					
					// if the closest peer is me, set myself as the nextEntry, otherwise is the peer in my right leaf
					if (closestPeer.equals(this.peerNodeIdentifier)) {
						nextEntry = this.peerTableEntry;
					} else {
						nextEntry = this.rightLeafTableEntry;
					}
				}
			// joining node is equal distance from both leaf sets
			} else {
				int index = getIndexOfPrefixNotMatching(newIdentifier, this.peerNodeIdentifier);
				String lookupEntryString = newIdentifier.substring(0, index + 1);
				
				synchronized (this.routingTable) {
					HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(index);
					nextEntry = row.get(lookupEntryString);
					
					// make sure that this is an actual entry in the table and not null
					if (nextEntry != null) {
						try {
							Socket socket = new Socket(nextEntry.getNodeInformation().getNodeIPAddress(), nextEntry.getNodeInformation().getNodePortNumber());
						// if we throw an exception, nextEntry is null and update the routing table accordingly
						} catch (IOException ioe) {
							row.put(lookupEntryString, null);
							nextEntry = null;
						}
					}
				}
			}
			
			// if all attempts have failed and we're still left with a null node, THEN
			if (nextEntry == null) {
				nextEntry = this.peerTableEntry;
				closestPeer = this.peerNodeIdentifier;
				
				synchronized (this.routingTable) {
					int routingTableSize = this.routingTable.getSizeOfRoutingTable();
					
					for (int i=0; i < routingTableSize; i++) {
						HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(i);
						
						for (Map.Entry<String, TableEntry> entrySet : row.entrySet()) {
							String identifier = entrySet.getKey();
							TableEntry entry = entrySet.getValue();
							
							if (entry != null) {
								try {
									Socket socket = new Socket(entry.getNodeInformation().getNodeIPAddress(), entry.getNodeInformation().getNodePortNumber());
								} catch (IOException ioe) {
									row.put(identifier, null);
									continue;
								}
								
								String entryIdentifier = entry.getIdentifier();
								closestPeer = getClosestPeer(newIdentifier, this.peerNodeIdentifier, entryIdentifier);
								
								if (closestPeer.equals(entryIdentifier)) {
									nextEntry = entry;
								}
							}
						}
					}
				}
				
				synchronized (this.leftLeafTableEntry) {
					closestPeer = getClosestPeer(newIdentifier, closestPeer, this.leftLeafTableEntry.getIdentifier());
				}
				
				synchronized (this.rightLeafTableEntry) {
					closestPeer = getClosestPeer(newIdentifier, closestPeer, this.rightLeafTableEntry.getIdentifier());
				}
				
				synchronized (this.leftLeafTableEntry) {
					if (closestPeer.equals(this.leftLeafTableEntry.getIdentifier())) {
						nextEntry = this.leftLeafTableEntry;
					}
				}
				
				synchronized (this.rightLeafTableEntry) {
					if (closestPeer.equals(this.rightLeafTableEntry.getIdentifier())) {
						nextEntry = this.rightLeafTableEntry;
					}
				}
				
				if (closestPeer.equals(this.peerNodeIdentifier)) {
					nextEntry = this.peerTableEntry;
				}
			}
		}
		
		return nextEntry;
	}
	
	private int distanceCounterClockwiseSearch(String identifier1, String identifier2) {
		int distance = -1;
		int distance1 = Integer.parseInt(identifier1, 16);
		int distance2 = Integer.parseInt(identifier2, 16);
		
		// node we are getting distance to is beyond the 0 position
		if (distance1 < distance2) {
			distance = (SIZE_OF_CHUNK - distance2) + distance1;
		// both nodes are on the same side of the circle
		} else {
			distance = distance1 - distance2;
		}
		
		return distance;
	}
	
	private int distanceClockwiseSearch(String identifier1, String identifier2) {
		int distance = -1;
		int distance1 = Integer.parseInt(identifier1, 16);
		int distance2 = Integer.parseInt(identifier2, 16);
		
		// node we are getting distance to is behind us on the same side of the circle
		if (distance1 < distance2) {
			distance = distance2 - distance1;
		// node we are getting distance is beyond the 0 position
		} else {
			distance = (SIZE_OF_CHUNK - distance1) + distance2;
		}
		
		return distance;
	}
	
	private int getPeerNodesDistance(String identifier1, String identifier2) {
		int distance = -1;
		int distance1 = Integer.parseInt(identifier1, 16);
		int distance2 = Integer.parseInt(identifier2, 16);
		
		int absoluteDistance = Math.abs(distance1 - distance2);
		int spaceDistance = SIZE_OF_CHUNK - absoluteDistance;
		
		// node we are getting distance to is beyond the 0 position
		if (distance1 < distance2) {
			// node1 is closer to node2 going clockwise
			if (absoluteDistance < spaceDistance) {
				distance = absoluteDistance;
			// // node1 is closer to node2 going counter-clockwise
			} else {
				distance = spaceDistance;
			}
		// both nodes are on the same side of the circle
		} else {
			// node1 is closer to node2 going counter-clockwise
			if (absoluteDistance < spaceDistance) {
				distance = absoluteDistance;
			// node1 is closer to node2 going clockwise
			} else {
				distance = spaceDistance;
			}
		}
		
		return distance;
	}
	
	private Direction getDirection(String identifier1, String identifier2) {
		Direction direction = null;
		
		int distance1 = Integer.parseInt(identifier1, 16);
		int distance2 = Integer.parseInt(identifier2, 16);
		
		int absoluteDistance = Math.abs(distance1 - distance2);
		int spaceDistance = SIZE_OF_CHUNK - absoluteDistance;
		
		// node we are getting distance to is beyond the 0 position
		if (distance1 < distance2) {
			// node1 is closer to node2 going clockwise
			if (absoluteDistance < spaceDistance) {
				direction = Direction.CLOCKWISE;
			// // node1 is closer to node2 going counter-clockwise
			} else {
				direction = Direction.COUNTER_CLOCKWISE;
			}
		// both nodes are on the same side of the circle
		} else {
			// node1 is closer to node2 going counter-clockwise
			if (absoluteDistance < spaceDistance) {
				direction = Direction.COUNTER_CLOCKWISE;
			// node1 is closer to node2 going clockwise
			} else {
				direction = Direction.CLOCKWISE;
			}
		}
		
		return direction;
	}
	
	private String getClosestPeer(String centeredIdentifier, String identifier1, String identifier2) {
		String closestPeer = null;
		
		int distanceFromIdentiefer1 = getPeerNodesDistance(centeredIdentifier, identifier1);
		int distanceFromIdentiefer2 = getPeerNodesDistance(centeredIdentifier, identifier2);
		
		// identifier1 is closer to us 
		if (distanceFromIdentiefer1 < distanceFromIdentiefer2) {
			closestPeer = identifier1;
		// identifier2 is closer to us 
		} else if (distanceFromIdentiefer1 > distanceFromIdentiefer2) {
			closestPeer = identifier2;
		// identifiers are equal distance to us, need to find node that is before us when traversing clockwise
		} else {
			Direction directionToIdentifier1 = getDirection(centeredIdentifier, identifier1);
			Direction directionToIdentifier2 = getDirection(centeredIdentifier, identifier2);
			
			// identifier1 is past us
			if (directionToIdentifier1.equals(Direction.CLOCKWISE)) {
				closestPeer = identifier1;
			// identifier2 is past us
			} else if (directionToIdentifier2.equals(Direction.CLOCKWISE)){
				closestPeer = identifier2;
			}
			
		}
		
		return closestPeer;
	}
	
	private int getIndexOfPrefixNotMatching(String identifier1, String identifier2) {
		int index = 0;
		int loopCount = 0;
		
		if (identifier1.length() < identifier2.length()) {
			loopCount = identifier1.length();
		} else {
			loopCount = identifier2.length();
		}
		
		// find the first index where the identifiers no longer match
		for (int i=0; i < loopCount; i++) {
			if (identifier1.charAt(i) != identifier2.charAt(i)) {
				index = i;
				break;
			}
		}
		
		return index;
		
	}
	
	private void generateEntriesInRoutingTable(String newIdentifier, RoutingTable newRoutingTable) {
		int loopCount = getIndexOfPrefixNotMatching(this.peerNodeIdentifier, newIdentifier);
		
		synchronized (this.routingTable) {
			for (int i=0; i < loopCount; i++) {
				HashMap<String, TableEntry> row = this.routingTable.getRoutingTableEntry(i);
				HashMap<String, TableEntry> newRow = newRoutingTable.getRoutingTableEntry(i);
				
				for (Map.Entry<String, TableEntry> entrySet : row.entrySet()) {
					String identifier = entrySet.getKey();
					TableEntry entry = entrySet.getValue();
					
					if (entry != null) {
						if (newRow.get(identifier) == null) {
							newRow.put(identifier, entry);
						}
					}
				}
				
				String missingEntryIdentifier = this.peerNodeIdentifier.substring(0, i + 1);
				
				if (newRow.get(missingEntryIdentifier) == null) {
					newRow.put(missingEntryIdentifier, this.peerTableEntry);
				}
			}
		}
	}
	
	private void saveFile(String fileName, String key, byte[] fileData) {

		String path = this.storedFileLocation + fileName;
		File fileLocationToBeSaved = new File(path.substring(0, path.lastIndexOf("/")));
		
		if (!fileLocationToBeSaved.exists()) {
			fileLocationToBeSaved.mkdirs();
		}
		
		File fileToBeSaved = new File(path);
		
		try {
			// save file to the local system
			FileOutputStream fos = new FileOutputStream(fileToBeSaved);
			fos.write(fileData, 0, fileData.length);
			
			System.out.println("Saving file to the following location: " + fileToBeSaved.getAbsolutePath());
			
			synchronized (this.filesWithKeyMap) {
				this.filesWithKeyMap.put(fileName, key);
			}
			
			fos.close();
			
		} catch (FileNotFoundException e) {
			System.out.println("ChunkServer: Error in saveFile: File location not found.");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("ChunkServer: Error in saveFile: Writing file failed.");
			e.printStackTrace();
		}
	}
	
	private byte[] readAndGetFile(String fileName, String key) {
		String fileLocation = this.storedFileLocation + fileName;
		File fileToReturn = new File(fileLocation);
		
		Boolean exists = false;
		
		synchronized (this.filesWithKeyMap) {
			if (this.filesWithKeyMap.containsKey(fileName)) {
				String tempKey = this.filesWithKeyMap.get(fileName);
				if (tempKey.equalsIgnoreCase(key)) {
					exists = true;
				}
			}
		}
		
		if (fileToReturn.exists() && exists) {
			try {
				RandomAccessFile raf = new RandomAccessFile(fileToReturn, "rw");
				byte[] tempData = new byte[(int) fileToReturn.length()];
				raf.read(tempData);
				
				raf.close();
				
				return tempData;
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("ERROR: File could not be found!");
		}
		
		byte[] nullBytes = null;
		return nullBytes;
	}
	
	private void removePeer() {
		PeerRemovePeerToDiscovery removePeer = new PeerRemovePeerToDiscovery(this.peerTableEntry);
		
		// first notify the discovery node that we are removing this peer from the DHT
		try {
			this.discoverySender.sendData(removePeer.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (this.initialized.get()) {
			
			TableEntry leftLeaf = null;
			synchronized (this.leftLeafTableEntry) {
				leftLeaf = this.leftLeafTableEntry;
			}
			
			TableEntry rightLeaf = null;
			synchronized (this.rightLeafTableEntry) {
				rightLeaf = this.rightLeafTableEntry;
			}
			
			if (leftLeaf != null && rightLeaf != null) {
				// notify the left leafset that we are being removed from the system
				System.out.println("[INFO] Notifying Left Leaf Node");
				PeerRemoveToLeftLeafPeer removeToLeft = new PeerRemoveToLeftLeafPeer(rightLeaf);
				
				try {
					Socket socket = new Socket(leftLeaf.getNodeInformation().getNodeIPAddress(), leftLeaf.getNodeInformation().getNodePortNumber());
					TCPSender sender = new TCPSender(socket);
					sender.sendData(removeToLeft.getBytes());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				// notify the right leafset that we are being removed from the system
				System.out.println("[INFO] Notifying Right Leaf Node");
				PeerRemoveToRightLeafPeer removeToRight = new PeerRemoveToRightLeafPeer(leftLeaf);
				
				try {
					Socket socket = new Socket(rightLeaf.getNodeInformation().getNodeIPAddress(), rightLeaf.getNodeInformation().getNodePortNumber());
					TCPSender sender = new TCPSender(socket);
					sender.sendData(removeToRight.getBytes());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			// send all files stored at this peer to whatever peer is closer, left or right leaf
			System.out.println("[INFO] Transfering Files");
			synchronized (this.filesWithKeyMap) {
				for (String s : this.filesWithKeyMap.keySet()) {
					String key = this.filesWithKeyMap.get(s);
					
					TableEntry newTableEntry = null;
					
					String newIdentifier = getClosestPeer(key, leftLeaf.getIdentifier(), rightLeaf.getIdentifier());
					
					if (newIdentifier.equals(leftLeaf.getIdentifier())) {
						newTableEntry = leftLeaf;
					} else if (newIdentifier.equals(rightLeaf.getIdentifier())) {
						newTableEntry = rightLeaf;
					}
					
					if (newTableEntry != null) {
						byte[] fileBytes = readAndGetFile(s, key);
						PeerForwardFileToPeer forwardFile = new PeerForwardFileToPeer(s, key, fileBytes, this.peerTableEntry);
						
						try {
							Socket socket = new Socket(newTableEntry.getNodeInformation().getNodeIPAddress(), newTableEntry.getNodeInformation().getNodePortNumber());
							TCPSender sender = new TCPSender(socket);
							sender.sendData(forwardFile.getBytes());
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			
			// notify all nodes to remove this peer from the routing table
			System.out.println("[INFO] Notifying Routing table");
			
			synchronized (this.routingTable) {
				for (Entry<Integer, HashMap<String, TableEntry>> entrySet : this.routingTable.getTable().entrySet()) {
	                //int key = entrySet.getKey();
	                HashMap<String, TableEntry> row = entrySet.getValue();

                    for (Map.Entry<String, TableEntry> entrySet1 : row.entrySet()) {
                        //String identifier = entrySet1.getKey();
                        TableEntry peer = entrySet1.getValue();
                        if (peer != null) {
                            try {
                            	PeerRemoveFromRoutingTableToPeer removeFromRoutingTable = new PeerRemoveFromRoutingTableToPeer(this.peerTableEntry);
                                Socket socket = new Socket(peer.getNodeInformation().getNodeIPAddress(), peer.getNodeInformation().getNodePortNumber());
                                
                                TCPSender sender = new TCPSender(socket);
                                
                                sender.sendData(removeFromRoutingTable.getBytes());
                            } catch (Exception ex) {

                            }
                        }
                    }
                }
			}
		}
		
		System.exit(0);
	}
	
	private void printRoutingTable() {
		synchronized (this.routingTable) {
			this.routingTable.printRoutingTable();
		}
		//handleUserInput();
	}
	
	private void printLeafSet() {
		synchronized (this.leftLeafTableEntry) {
			if (this.leftLeafTableEntry != null) {
				System.out.println("[INFO] LEFTLEAF: " + this.leftLeafTableEntry);
			} else {
				System.out.println("[INFO] LEFTLEAF: No Left Leaf TableEntry registered.");
			}
		}
		
		synchronized (this.rightLeafTableEntry) {
			if (this.rightLeafTableEntry != null) {
				System.out.println("[INFO] RIGHTLEAF: " + this.rightLeafTableEntry);
			} else {
				System.out.println("[INFO] RIGHTLEAF: No Right Leaf TableEntry registered.");
			}
		}
		//handleUserInput();
	}
	
	private void printFilesList() {
		synchronized (this.filesWithKeyMap) {
			System.out.println("[INFO] ---Stored Files---");
			for (String s : this.filesWithKeyMap.keySet()) {
				System.out.println("[INFO] Filename: " + s + " - Key: " + this.filesWithKeyMap.get(s));
			}
		}
		//handleUserInput();
	}
	
	private void sendPeerLeftLeafRouteTraceToPeer() {
		String originNodeIdentifier = this.peerNodeIdentifier;
		ArrayList<String> traceIdentifierList = new ArrayList<String>();
		
		traceIdentifierList.add(this.peerNodeIdentifier);
		
		PeerLeftLeafRouteTraceToPeer leftLeafRouteTrace = new PeerLeftLeafRouteTraceToPeer(originNodeIdentifier, traceIdentifierList.size(), traceIdentifierList);
		
		synchronized (this.leftLeafTableEntry) {
			try {
				Socket socket = new Socket(this.leftLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.leftLeafTableEntry.getNodeInformation().getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(leftLeafRouteTrace.getBytes());
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private void handlePeerLeftLeafRouteTraceToPeer(Event event) {
		PeerLeftLeafRouteTraceToPeer leftLeafRouteTrace = (PeerLeftLeafRouteTraceToPeer) event;
		
		String originNodeIdentifier = leftLeafRouteTrace.getOriginNodeIdentifier();
		ArrayList<String> traceIdentifierList = leftLeafRouteTrace.getTraceIdentifierList();
		
		if (originNodeIdentifier.equals(this.peerNodeIdentifier)) {
			System.out.println("[INFO] ---BACK TO ORIGIN---");
			
			String traceString = "";
	        for (String s : traceIdentifierList) {
	        	traceString = traceString + s + "-";
	        }
	        
	        traceString = traceString.substring(0, traceString.length() - 1);
	        System.out.println("[INFO] LEFT LEAF TRACE: " + traceString);
	        
	        // make it easier and see the hex values as decimal
	        ArrayList<Long> traceAsLongList = new ArrayList<Long>();
	        
	        for (String s : traceIdentifierList) {
	        	traceAsLongList.add(Long.parseLong(s, 16));
	        }
	        
	        String traceLongString = "";
	        for (Long l : traceAsLongList) {
	        	traceLongString = traceLongString + l + "-";
	        }
	        
	        traceLongString = traceLongString.substring(0, traceLongString.length() - 1);
	        System.out.println("[INFO] LEFT LEAF TRACE AS DECIMAL: " + traceLongString);
	        
	        //handleUserInput();
		} else {
			System.out.println("[INFO] ---FORWARDING LEFT LEAF ROUTE TRACE---");
			traceIdentifierList.add(this.peerNodeIdentifier);
			
			synchronized (this.leftLeafTableEntry) {
				try {
					PeerLeftLeafRouteTraceToPeer leftLeafRouteTraceForward = new PeerLeftLeafRouteTraceToPeer(originNodeIdentifier, traceIdentifierList.size(), traceIdentifierList);
					
					Socket socket = new Socket(this.leftLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.leftLeafTableEntry.getNodeInformation().getNodePortNumber());
					
					TCPSender sender = new TCPSender(socket);
					sender.sendData(leftLeafRouteTraceForward.getBytes());
					
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void sendPeerRightLeafRouteTraceToPeer() {
		String originNodeIdentifier = this.peerNodeIdentifier;
		ArrayList<String> traceIdentifierList = new ArrayList<String>();
		
		traceIdentifierList.add(this.peerNodeIdentifier);
		
		PeerRightLeafRouteTraceToPeer rightLeafRouteTrace = new PeerRightLeafRouteTraceToPeer(originNodeIdentifier, traceIdentifierList.size(), traceIdentifierList);
		
		synchronized (this.rightLeafTableEntry) {
			try {
				Socket socket = new Socket(this.rightLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.rightLeafTableEntry.getNodeInformation().getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(rightLeafRouteTrace.getBytes());
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private void handlePeerRightLeafRouteTraceToPeer(Event event) {
		PeerRightLeafRouteTraceToPeer rightLeafRouteTrace = (PeerRightLeafRouteTraceToPeer) event;
		
		String originNodeIdentifier = rightLeafRouteTrace.getOriginNodeIdentifier();
		ArrayList<String> traceIdentifierList = rightLeafRouteTrace.getTraceIdentifierList();
		
		if (originNodeIdentifier.equals(this.peerNodeIdentifier)) {
			System.out.println("[INFO] ---BACK TO ORIGIN---");
			
			String traceString = "";
	        for (String s : traceIdentifierList) {
	        	traceString = traceString + s + "-";
	        }
	        
	        traceString = traceString.substring(0, traceString.length() - 1);
	        System.out.println("[INFO] RIGHT LEAF TRACE: " + traceString);
	        
	     // make it easier and see the hex values as decimal
	        ArrayList<Long> traceAsLongList = new ArrayList<Long>();
	        
	        for (String s : traceIdentifierList) {
	        	traceAsLongList.add(Long.parseLong(s, 16));
	        }
	        
	        String traceLongString = "";
	        for (Long l : traceAsLongList) {
	        	traceLongString = traceLongString + l + "-";
	        }
	        
	        traceLongString = traceLongString.substring(0, traceLongString.length() - 1);
	        System.out.println("[INFO] RIGHT LEAF TRACE AS DECIMAL: " + traceLongString);
	        
	        
	        //handleUserInput();
		} else {
			System.out.println("[INFO] ---FORWARDING RIGHT LEAF ROUTE TRACE---");
			traceIdentifierList.add(this.peerNodeIdentifier);
			
			synchronized (this.rightLeafTableEntry) {
				try {
					PeerRightLeafRouteTraceToPeer rightLeafRouteTraceForward = new PeerRightLeafRouteTraceToPeer(originNodeIdentifier, traceIdentifierList.size(), traceIdentifierList);
					
					Socket socket = new Socket(this.rightLeafTableEntry.getNodeInformation().getNodeIPAddress(), this.rightLeafTableEntry.getNodeInformation().getNodePortNumber());
					
					TCPSender sender = new TCPSender(socket);
					sender.sendData(rightLeafRouteTraceForward.getBytes());
					
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
