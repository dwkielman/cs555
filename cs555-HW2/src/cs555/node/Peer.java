package cs555.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import cs555.transport.TCPReceiverThread;
import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.Direction;
import cs555.util.NodeInformation;
import cs555.util.TableEntry;
import cs555.wireformats.DiscoveryRegisterResponseToPeer;
import cs555.wireformats.DiscoverySendRandomNodeToPeer;
import cs555.wireformats.Event;
import cs555.wireformats.PeerJoinRequestToPeer;
import cs555.wireformats.PeerRegisterRequestToDiscovery;
import cs555.wireformats.Protocol;

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
	private TableEntry peerTableEntry;
	private TableEntry leftLeafTableEntry;
	private TableEntry rightLeafTableEntry;
	private static Peer peer;
	
	private static final int SIZE_OF_CHUNK = 1024 * 64;
	
	private Peer(String controllerIPAddress, int controllerPortNumber, int peerPortNumber, String peerIdentifer) {
		this.discoveryNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		this.peerNodeIdentifier = peerIdentifer;
		this.initialized = new AtomicBoolean(false);
		this.leftLeafTableEntry = null;
		this.rightLeafTableEntry = null;
		
		try {
			TCPServerThread serverThread = new TCPServerThread(peerPortNumber, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			
			this.peerTableEntry = new TableEntry(peerNodeIdentifier, discoveryNodeInformation, localHostIPAddress);
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		this.peerNodeInformation = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
		// Once the initialization is complete, client should send a registration request to the controller.
		connectToDisocvery();
	}
	
	public static void main(String[] args) {
			
		String discoveryIPAddress = args[0];
		int discoveryPortNumber = 0;
		int peerPortNumber = 0;
		String peerIdentifer = null;
		
		// requires 3 argument to initialize a peer
		if (args.length == 3) {
			peerIdentifer = generateIdentifer();
		}
		if (args.length == 4) {
			peerIdentifer = args[3];
        } else if (args.length != 3) {
        	System.out.println("Invalid Arguments. Must include a Discovery IP Address, Port Number, Peer's Port Number and an optional identifier.");
            return;
        }
		
		try {
			discoveryPortNumber = Integer.parseInt(args[1]);
			peerPortNumber = Integer.parseInt(args[2]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Arguments must be a number.");
			nfe.printStackTrace();
		}
		peer = new Peer(discoveryIPAddress, discoveryPortNumber, peerPortNumber, peerIdentifer);
		
	}
	
	private static String generateIdentifer() {
		Long l1 = System.nanoTime();
        String hex = Long.toHexString(l1);
        String randomID = hex.substring(hex.length() - 4);
        randomID = randomID.toUpperCase();
        return randomID;
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
	
	private void connectToDisocvery() {
		if (DEBUG) { System.out.println("begin Peer connectToDisocvery"); }
		try {
			System.out.println("Attempting to connect to Controller " + this.discoveryNodeInformation.getNodeIPAddress() + " at Port Number: " + this.discoveryNodeInformation.getNodePortNumber());
			Socket discoverySocket = new Socket(this.discoveryNodeInformation.getNodeIPAddress(), this.discoveryNodeInformation.getNodePortNumber());
			
			System.out.println("Starting TCPReceiverThread with Controller");
			peerReceiverThread = new TCPReceiverThread(discoverySocket, this);
			Thread tcpReceiverThread = new Thread(this.peerReceiverThread);
			tcpReceiverThread.start();
			
			System.out.println("TCPReceiverThread with Controller started");
			System.out.println("Sending to " + this.discoveryNodeInformation.getNodeIPAddress() + " on Port " +  this.discoveryNodeInformation.getNodePortNumber());
			
			this.discoverySender = new TCPSender(discoverySocket);

			PeerRegisterRequestToDiscovery peerRegisterRequest = new PeerRegisterRequestToDiscovery(this.peerTableEntry);

			if (DEBUG) { System.out.println("ChunkServer about to send message type: " + peerRegisterRequest.getType()); }
			
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
			this.initialized.getAndSet(true);
			System.out.println("Registration Request Succeeded.");
			System.out.println(String.format("Message: %s", peerRegisterResponse.getAdditionalInfo()));
		// unsuccessful registration due to conflict with duplicate identifier
		} else if (peerRegisterResponse.getStatusCode() == (byte) 2) {
			System.out.println("Registration Request Failed due to duplicate identifer in system. Generating new one and attempting registration.");
			String newIdentifier = generateIdentifer();
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
		
		PeerJoinRequestToPeer joinRequest = new PeerJoinRequestToPeer(this.peerTableEntry, 1, traceList, 0);
		
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
		
		TableEntry joiningTableEntry = joinRequest.getTableEntry();
		int hopCount = joinRequest.getHopCount();
		hopCount++;
		
		// lookup(joiningTableEntry.getIdentifer());
		
		if (DEBUG) { System.out.println("end Peer handlePeerJoinRequestToPeer"); }
	}
	
	private TableEntry lookup(String newIdentifer) {
		TableEntry nextEntry = null;
		
		// if no other entries on one side then this is the next entry
		if (this.leftLeafTableEntry == null || this.rightLeafTableEntry == null) {
			nextEntry = this.peerTableEntry;
		} else {
			int currentDistanceLeft, currentDistanceRight;
			
			// collect our current distance from the node stored in the leafset for this node
			synchronized (this.leftLeafTableEntry) {
				currentDistanceLeft = distanceCounterClockwiseSearch(this.peerNodeIdentifier, this.leftLeafTableEntry.getIdentifier());
			}
			
			synchronized (this.rightLeafTableEntry) {
				currentDistanceRight = distanceCounterClockwiseSearch(this.peerNodeIdentifier, this.rightLeafTableEntry.getIdentifier());
			}
			
			// gather the distance for the joining node from this node
			int joiningDistanceLeft = distanceCounterClockwiseSearch(this.peerNodeIdentifier, newIdentifer);
			int joiningDistanceRight = distanceCounterClockwiseSearch(this.peerNodeIdentifier, newIdentifer);
			
			// joining node is closer than the current node stored in the left leaf set
			if (joiningDistanceLeft < currentDistanceLeft) {
				// find which is the closest node, either myself or the node in the left leaf
			} else if (joiningDistanceRight < currentDistanceRight) {
				// find which is the closest node, either myself or the node in the right leaf
				
				// should use the same logic as above just with right leaf instead
			} else {
				// distance is dead equal
			}
		}
		
		
		return nextEntry;
	}
	
	private int distanceCounterClockwiseSearch(String identifer1, String identifer2) {
		int distance = -1;
		int distance1 = Integer.parseInt(identifer1, 16);
		int distance2 = Integer.parseInt(identifer2, 16);
		
		// node we are getting distance to is beyond the 0 position
		if (distance1 < distance2) {
			distance = (SIZE_OF_CHUNK - distance2) + distance1;
		// both nodes are on the same side of the circle
		} else {
			distance = distance1 - distance2;
		}
		
		return distance;
	}
	
	private int distanceClockwiseSearch(String identifer1, String identifer2) {
		int distance = -1;
		int distance1 = Integer.parseInt(identifer1, 16);
		int distance2 = Integer.parseInt(identifer2, 16);
		
		// node we are getting distance to is behind us on the same side of the circle
		if (distance1 < distance2) {
			distance = distance2 - distance1;
		// node we are getting distance is beyond the 0 position
		} else {
			distance = (SIZE_OF_CHUNK - distance1) + distance2;
		}
		
		return distance;
	}
	
	private int getPeerNodesDistance(String identifer1, String identifer2) {
		int distance = -1;
		int distance1 = Integer.parseInt(identifer1, 16);
		int distance2 = Integer.parseInt(identifer2, 16);
		
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
	
	private Direction getDirection(String identifer1, String identifer2) {
		Direction direction = null;
		
		int distance1 = Integer.parseInt(identifer1, 16);
		int distance2 = Integer.parseInt(identifer2, 16);
		
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
}
