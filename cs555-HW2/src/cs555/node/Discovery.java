package cs555.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.NodeInformation;
import cs555.util.TableEntry;
import cs555.wireformats.DiscoveryReadRequestResponseToStoreData;
import cs555.wireformats.DiscoveryRegisterResponseToPeer;
import cs555.wireformats.DiscoverySendRandomNodeToPeer;
import cs555.wireformats.DiscoveryStoreRequestResponseToStoreData;
import cs555.wireformats.Event;
import cs555.wireformats.PeerRegisterRequestToDiscovery;
import cs555.wireformats.PeerRemovePeerToDiscovery;
import cs555.wireformats.PeerUpdateCompleteAndInitializedToDiscovery;
import cs555.wireformats.Protocol;
import cs555.wireformats.StoreDataReadRequestToDiscovery;
import cs555.wireformats.StoreDataStoreRequestToDiscovery;

public class Discovery implements Node {

	private static final boolean DEBUG = false;
	private int portNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private HashMap<NodeInformation, TCPSender> peerNodesMap;
	private static Discovery discovery;
	private final static Logger LOGGER = Logger.getLogger(Discovery.class.getName());
	private HashMap<String, TableEntry> nodeTableEntryMap;
	
	private Discovery(int portNumber) {
		this.portNumber = portNumber;
		this.peerNodesMap = new HashMap<NodeInformation, TCPSender>();
		this.nodeTableEntryMap = new HashMap<String, TableEntry>();
		
		try {
			TCPServerThread discoveryServerThread = new TCPServerThread(this.portNumber, this);
			this.tCPServerThread = discoveryServerThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			//System.out.println("Discovery TCPServerThread running.");
			LOGGER.fine("Discovery TCPServerThread running.");
			
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, null, e);
			e.printStackTrace();
		}
	}
	
	public Discovery() {}
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a discovery node
		if(args.length != 1) {
			//System.out.println("Invalid Arguments. Must include a port number.");
			LOGGER.severe("Invalid Arguments. Must include a port number.");
            return;
        }
		
		int discoveryPortNumber = 0;
		
		try {
			discoveryPortNumber = Integer.parseInt(args[0]);
		} catch (NumberFormatException nfe) {
			//System.out.println("Invalid argument. Argument must be a number.");
			LOGGER.severe("Invalid argument. Argument must be a number.");
			nfe.printStackTrace();
		}
		
		String discoveryIP = "";
		
        try{
        	discoveryIP = InetAddress.getLocalHost().getHostAddress();
        	if (DEBUG) { System.out.println("Trying to get discoveryIP"); }
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }
        
       // System.out.println("Discovery is running at IP Address: " + discoveryIP + " on Port Number: " + discoveryPortNumber);
        LOGGER.info("Discovery is running at IP Address: " + discoveryIP + " on Port Number: " + discoveryPortNumber);
        discovery = new Discovery(discoveryPortNumber);
        discovery.handleUserInput();
	}
	
	@Override
	public synchronized void onEvent(Event event) {
		int eventType = event.getType();
		if (DEBUG) { System.out.println("Event Type " + eventType + " passed to Discovery."); }
		switch(eventType) {
			// PEER_REGISTER_REQUEST_TO_DISCOVERY = 7000
			case Protocol.PEER_REGISTER_REQUEST_TO_DISCOVERY:
				handlePeerRegisterRequestToDiscovery(event);
				break;
			// PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY = 7014
			case Protocol.PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY:
				handlePeerUpdateCompleteAndInitializedToDiscovery(event);
				break;
			// PEER_REMOVE_PEER_TO_DISCOVERY = 7015
			case Protocol.PEER_REMOVE_PEER_TO_DISCOVERY:
				handlePeerRemovePeerToDiscovery(event);
				break;
			// STOREDATA_STORE_REQUEST_TO_DISCOVERY = 8000
			case Protocol.STOREDATA_STORE_REQUEST_TO_DISCOVERY:
				handleStoreDataStoreRequestToDiscovery(event);
				break;
			// STOREDATA_READ_REQUEST_TO_DISCOVERY = 8003
			case Protocol.STOREDATA_READ_REQUEST_TO_DISCOVERY:
				handleStoreDataReadRequestToDiscovery(event);
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
	
	private void handleUserInput() {
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Ready for input.");
			
        while(true) {
            System.out.println("Options:\n[list-nodes] List Nodes in the system\n[Q] Quit\nPlease type your request: ");
            String input = scan.nextLine();
            
            //input = input.toUpperCase();
            switch (input) {
            	case "list-nodes":
            		if (DEBUG) { System.out.println("User selected list-nodes"); }
            		listNodes();
            		break;
            	case "q":
            		if (DEBUG) { System.out.println("User selected Quit."); }
            		System.out.println("Quitting program. Goodbye.");
            		System.exit(1);
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
		
		//System.out.println("Discovery received a PeerRegisterRequestToDiscovery from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		LOGGER.info("Discovery received a PeerRegisterRequestToDiscovery from IP: " + IP + " on Port: " + String.valueOf(port) + ".");
		
		//NodeInformation ni = new NodeInformation(IP, port);

		// need to now perform some functionality to add a new peer node to the table carefully
		registerNewNode(peerRegisterRequest.getTableEntry());

		if (DEBUG) { System.out.println("end Discovery handlePeerRegisterRequestToDiscovery"); }
	}
	
	private void handleStoreDataStoreRequestToDiscovery(Event event) {
		if (DEBUG) { System.out.println("begin Discovery handleStoreDataStoreRequestToDiscovery"); }
		StoreDataStoreRequestToDiscovery storeDataStoreRequest = (StoreDataStoreRequestToDiscovery) event;
		
		// get a random node to send back to the StoreData node
		String randNodeIdentifier = getRandomNode();
		
		if (randNodeIdentifier != null) {
			TableEntry registeredEntry  = null;
			synchronized (this.nodeTableEntryMap) {
				registeredEntry = nodeTableEntryMap.get(randNodeIdentifier);
			}
			
			NodeInformation storeData = storeDataStoreRequest.getStoreData();
			String filename = storeDataStoreRequest.getFilename();
			String key = storeDataStoreRequest.getKey();

			//System.out.println("Discovery received request to Store Data from StoreData at: hostname: " + storeData.getNodeIPAddress() + " : port: " + storeData.getNodePortNumber() + "\nSending Random Peer: " + registeredEntry);
			LOGGER.info("Discovery received request to Store Data from StoreData at: hostname: " + storeData.getNodeIPAddress() + " : port: " + storeData.getNodePortNumber() + "\nSending Random Peer: " + registeredEntry);
			
			DiscoveryStoreRequestResponseToStoreData storeResponse = new DiscoveryStoreRequestResponseToStoreData(filename, key, registeredEntry.getNodeInformation());
			
			try {
				Socket socket = new Socket(storeData.getNodeIPAddress(), storeData.getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(storeResponse.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			//System.out.println("No Peers available to send data to store to.");
			LOGGER.severe("No Peers available to store data to.");
		}

		if (DEBUG) { System.out.println("end Discovery handleStoreDataStoreRequestToDiscovery"); }
	}
	
	private void handleStoreDataReadRequestToDiscovery(Event event) {
		if (DEBUG) { System.out.println("begin Discovery handleStoreDataReadRequestToDiscovery"); }
		StoreDataReadRequestToDiscovery storeDataReadRequest = (StoreDataReadRequestToDiscovery) event;
		
		// get a random node to send back to the StoreData node
		String randNodeIdentifier = getRandomNode();
		
		if (randNodeIdentifier != null) {
			TableEntry registeredEntry  = null;
			synchronized (this.nodeTableEntryMap) {
				registeredEntry = nodeTableEntryMap.get(randNodeIdentifier);
			}
			
			NodeInformation storeData = storeDataReadRequest.getStoreData();
			String filename = storeDataReadRequest.getFilename();
			String key = storeDataReadRequest.getKey();

			//System.out.println("Discovery received request to Read Data from StoreData at: hostname: " + storeData.getNodeIPAddress() + " : port: " + storeData.getNodePortNumber() + "\nSending Random Peer: " + registeredEntry);
			LOGGER.info("Discovery received request to Read Data from StoreData at: hostname: " + storeData.getNodeIPAddress() + " : port: " + storeData.getNodePortNumber() + "\nSending Random Peer: " + registeredEntry);
			
			DiscoveryReadRequestResponseToStoreData readResponse = new DiscoveryReadRequestResponseToStoreData(filename, key, registeredEntry.getNodeInformation());
			
			try {
				Socket socket = new Socket(storeData.getNodeIPAddress(), storeData.getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				sender.sendData(readResponse.getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			//System.out.println("No Peers available to read data from.");
			LOGGER.severe("No Peers available to read data from.");
		}

		if (DEBUG) { System.out.println("end Discovery handleStoreDataReadRequestToDiscovery"); }
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
						
						//System.out.println("New Peer Request to Register from: " + te + ". Sending to Random Peer Identifier: " + randomNode);
						LOGGER.info("New Peer Request to Register from: " + te + "\nSending to Random Peer Identifier: " + randomNode);
						
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
						
						//System.out.println("New Peer Request to Register from: " + te + ". Sending No Random Peer due to this being the first Node in the System.");
						LOGGER.info("New Peer Request to Register from: " + te + "\nSending No Random Peer due to this being the first Node in the System.");
						
						sender = new TCPSender(socket);
						//peerNodesMap.put(te.getNodeInformation(), sender);
						
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
				//System.out.println("Node Identifier already exists in the map.");
				LOGGER.info("ERROR - DUPLICATE IDENTIFIER: New Peer Request to Register from: " + te + "\nNot Registering due to Identifier already exists.");
				// send a rejection to the peer so that it registers again
				
				//System.out.println("ERROR - DUPLICATE IDENTIFIER: New Peer Request to Register from: " + te + "\nNot Registering due to Identifier already exists.");
				
				status = 2;
				message = "Node Identifier already exists in the map";
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
	
	private void handlePeerUpdateCompleteAndInitializedToDiscovery(Event event) {
		if (DEBUG) { System.out.println("begin Discovery handlePeerUpdateCompleteAndInitializedToDiscovery"); }
		PeerUpdateCompleteAndInitializedToDiscovery updateComplete = (PeerUpdateCompleteAndInitializedToDiscovery) event;
		
		TableEntry updateEntry = updateComplete.getTableEntry();
		
		//System.out.println("NODE ADDITION SUCCESS: New Peer initialized and Ready for: " + updateEntry);
		LOGGER.severe("NODE ADDITION SUCCESS: New Peer initialized and Ready for: " + updateEntry);
		
		synchronized (this.nodeTableEntryMap) {
			this.nodeTableEntryMap.put(updateEntry.getIdentifier(), updateEntry);
		}
		
		synchronized (this.peerNodesMap) {
			try {
				Socket socket = new Socket(updateEntry.getNodeInformation().getNodeIPAddress(), updateEntry.getNodeInformation().getNodePortNumber());
				
				TCPSender sender = new TCPSender(socket);
				peerNodesMap.put(updateEntry.getNodeInformation(), sender);
				
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (DEBUG) { System.out.println("end Discovery handlePeerUpdateCompleteAndInitializedToDiscovery"); }
	}
	
	private void handlePeerRemovePeerToDiscovery(Event event) {
		if (DEBUG) { System.out.println("begin Discovery handlePeerRemovePeerToDiscovery"); }
		PeerRemovePeerToDiscovery removePeer = (PeerRemovePeerToDiscovery) event;
		
		TableEntry removedPeer = removePeer.getTableEntry();

		synchronized (this.nodeTableEntryMap) {
			if (this.nodeTableEntryMap.containsValue(removedPeer)) {
				this.nodeTableEntryMap.remove(removedPeer.getIdentifier());
				LOGGER.info("NODE REMOVAL: Peer being removed from Table Map: " + removedPeer);
				//System.out.println("NODE REMOVAL: Peer being removed from Table Map: " + removedPeer);
			}
		}
		
		synchronized (this.peerNodesMap) {
			if (this.peerNodesMap.containsKey(removedPeer.getNodeInformation())) {
				this.peerNodesMap.remove(removedPeer.getNodeInformation());
				LOGGER.info("NODE REMOVAL: Peer being removed from Nodes Map: " + removedPeer);
				//System.out.println("NODE REMOVAL: Peer being removed from Nodes Map: " + removedPeer);
			}
		}
		
		//System.out.println("NODE REMOVAL: Peer removed: " + removedPeer);
		LOGGER.severe("NODE REMOVAL: Peer removed: " + removedPeer);
		
		//handleUserInput();
		
		if (DEBUG) { System.out.println("end Discovery handlePeerRemovePeerToDiscovery"); }
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
        int totalNumberOfPeers = 0;
        synchronized (this.nodeTableEntryMap) {
        	if (!this.nodeTableEntryMap.isEmpty()) {
        		System.out.println("=================================================================");
                System.out.println("========================= LIST NODES ============================");
        		for (String s : this.nodeTableEntryMap.keySet()) {
            		TableEntry te = this.nodeTableEntryMap.get(s);
            		System.out.println("ENTRY " + (totalNumberOfPeers + 1) + ": " + te);
            		totalNumberOfPeers++;
            	}
        		System.out.println("=================================================================");
        		System.out.println("========= Total Number of Peer Nodes: " + totalNumberOfPeers + " ==================");
        		System.out.println("=================================================================");
        	} else {
        		System.out.println("Nodes are empty. No Peer Nodes Registered at this time.");
        	}
        }
        //handleUserInput();
	}
	
}
