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

import cs555.replication.transport.TCPControllerHeartbeat;
import cs555.replication.transport.TCPHeartbeat;
import cs555.replication.transport.TCPSender;
import cs555.replication.transport.TCPServerThread;
import cs555.replication.util.HeartbeatMetadata;
import cs555.replication.util.Metadata;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ChunkServerDeletedChunkToController;
import cs555.replication.wireformats.ChunkServerNotifyFixSuccessToController;
import cs555.replication.wireformats.ChunkServerRegisterRequestToController;
import cs555.replication.wireformats.ChunkServerSendCorruptChunkToController;
import cs555.replication.wireformats.ChunkServerSendMajorHeartbeatToController;
import cs555.replication.wireformats.ChunkServerSendMinorHeartbeatToController;
import cs555.replication.wireformats.ClientChunkServerRequestToController;
import cs555.replication.wireformats.ClientReadFileRequestToController;
import cs555.replication.wireformats.ClientRegisterRequestToController;
import cs555.replication.wireformats.ControllerChunkServerToReadResponseToClient;
import cs555.replication.wireformats.ControllerChunkServersResponseToClient;
import cs555.replication.wireformats.ControllerForwardDataToNewChunkServer;
import cs555.replication.wireformats.ControllerForwardFixCorruptChunkToChunkServer;
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
	private HashMap<NodeInformation, HeartbeatMetadata> chunkServerHeartbeatMetadaList;
	private ArrayList<NodeInformation> deadChunkServers;
	private HashMap<String, HashMap<Integer, ArrayList<HeartbeatMetadata>>> filesOnChunkServersMap;
	private HashMap<NodeInformation, HashMap<String, ArrayList<Integer>>> chunkServersWithFilesChunksMap;
	private HashMap<String, HashMap<Integer, ArrayList<NodeInformation>>> filesWithChunksNodeInformationMap;
	private static final int REPLICATION_LEVEL = 3;
	public static Controller controller;
	
	private Controller(int portNumber) {
		this.portNumber = portNumber;
		this.chunkServerNodesMap = new HashMap<NodeInformation, TCPSender>();
		this.chunkServerHeartbeatMetadaList = new  HashMap<NodeInformation, HeartbeatMetadata>();
		this.deadChunkServers = new ArrayList<NodeInformation>();
		this.filesOnChunkServersMap = new HashMap<String, HashMap<Integer, ArrayList<HeartbeatMetadata>>>();
		this.chunkServersWithFilesChunksMap = new HashMap<NodeInformation, HashMap<String, ArrayList<Integer>>>();
		this.filesWithChunksNodeInformationMap = new HashMap<String, HashMap<Integer, ArrayList<NodeInformation>>>();
		
		try {
			TCPServerThread controllerServerThread = new TCPServerThread(this.portNumber, this);
			this.tCPServerThread = controllerServerThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			System.out.println("Controller TCPServerThread running.");
			
			TCPControllerHeartbeat tCPControllerHeartbeat = new TCPControllerHeartbeat(controller);
			Thread tCPControllerHeartBeatThread = new Thread(tCPControllerHeartbeat);
			tCPControllerHeartBeatThread.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Controller() {}
	
	public void addDeadChunkServer(NodeInformation deadChunkServer) {
		synchronized (this.deadChunkServers) {
			this.deadChunkServers.add(deadChunkServer);
		}
	}
	
	public TCPSender getChunkServerSender(NodeInformation chunkServer) {
		synchronized (this.chunkServerNodesMap) {
			return this.chunkServerNodesMap.get(chunkServer);
		}
	}
	
	public Set<NodeInformation> getLiveChunkServers() {
		synchronized (this.chunkServerHeartbeatMetadaList) {
			return this.chunkServerHeartbeatMetadaList.keySet();
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
		
		controller = new Controller(controllerPortNumber);
		
		String controllerIP = "";
		
        try{
        	controllerIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Controller is running at IP Address: " + controllerIP + " on Port Number: " + controller.portNumber);
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
		HeartbeatMetadata hbm = new HeartbeatMetadata(ni, 0, nodeFreeSpace);

		try {
			Socket socket = new Socket(IP, port);
			TCPSender sender = new TCPSender(socket);
			
			byte status = 0;
			String message = "";
			
			// success, node is not currently registered so adding to the map of nodes
			if (!this.chunkServerNodesMap.containsKey(ni)) {
				this.chunkServerNodesMap.put(ni, sender);
				this.chunkServerHeartbeatMetadaList.put(ni, hbm);
				//this.chunkServerHeartbeatMetadaList.add(hbm);
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
		NodeInformation clientNode = clientChunkServerRequest.getClientNodeInformation();
		int chunkNumber = clientChunkServerRequest.getChunkNumber();
		String filename = clientChunkServerRequest.getFilename();
		long timestamp = clientChunkServerRequest.getTimestamp();
		
		if (DEBUG) { System.out.println("Controller received a message type: " + clientChunkServerRequest.getType()); }

		//if (filesWithChunksNodeInformationMap.containsKey(filename)) {	
			//HashMap<Integer, ArrayList<HeartbeatMetadata>> chunkServers = this.filesOnChunkServersMap.get(filename);
			if (chunkServerHeartbeatMetadaList.size() >= REPLICATION_LEVEL) {
				try {
					// sort the chunk servers by those with the most space, this should do it in descending order
					ArrayList<HeartbeatMetadata> tempHbmList = new ArrayList<HeartbeatMetadata>();
					for (HeartbeatMetadata hbm : this.chunkServerHeartbeatMetadaList.values()) {
						tempHbmList.add(hbm);
					}
					tempHbmList.sort((h1, h2) -> Long.compare(h2.getFreeSpaceAvailable(), h1.getFreeSpaceAvailable()));

					// get the 3 chunk servers with the most space
					ArrayList<HeartbeatMetadata> hbArrayList = tempHbmList.stream().limit(REPLICATION_LEVEL).collect(Collectors.toCollection(ArrayList::new));
					
					// prep chunkServers to send to Client
					ArrayList<NodeInformation> chunkServers = new ArrayList<NodeInformation>();
					
					for (HeartbeatMetadata hbm : hbArrayList) {
						chunkServers.add(hbm.getNodeInformation());
					}
					
					// update the chunkServers Hashmap
					synchronized (filesWithChunksNodeInformationMap) {
						if (filesWithChunksNodeInformationMap.containsKey(filename)) {	
							filesWithChunksNodeInformationMap.get(filename).put(chunkNumber, chunkServers);
						} else {
							HashMap<Integer, ArrayList<NodeInformation>> tempMap = new HashMap<Integer, ArrayList<NodeInformation>>();
							tempMap.put(chunkNumber, chunkServers);
							filesWithChunksNodeInformationMap.put(filename, tempMap);
						}
					}
					
					// update the chunkServers Hashmap
					/**
					synchronized (chunkServersWithFilesChunksMap) {
						for (NodeInformation cs : chunkServers) {
							if (chunkServersWithFilesChunksMap.get(cs).containsKey(filename)) {
								ArrayList<Integer> chunkNums = chunkServersWithFilesChunksMap.get(cs).get(filename);
								chunkNums.add(chunkNumber);
								chunkServersWithFilesChunksMap.put(cs, new HashMap<String, ArrayList<Integer>>());
							}
						}
					}
					
					HashMap<Integer, ArrayList<HeartbeatMetadata>> tempMap = new HashMap<Integer, ArrayList<HeartbeatMetadata>>(chunkNumber);
					tempMap.put(chunkNumber, hbArrayList);
					filesOnChunkServersMap.put(filename, tempMap);
					
					**/
					
					// put the chunkserver nodeinformation into a message and send to client
					ControllerChunkServersResponseToClient chunkServersResponse = new ControllerChunkServersResponseToClient(REPLICATION_LEVEL, chunkServers, chunkNumber, filename, timestamp);

					this.clientNodesMap.get(clientNode).sendData(chunkServersResponse.getBytes());
				} catch  (IOException ioe) {
					ioe.printStackTrace();
				}
			}
			// clean up, add to the relevant collections and be sure to update the free space available on the chunk servers that are being used
		//}
	}
	
	private void handleClientReadRequest(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleClientReadRequest"); }
		
		ClientReadFileRequestToController readRequest = (ClientReadFileRequestToController) event;
		String filename = readRequest.getFilename();
		NodeInformation clientNode = readRequest.getClienNodeInformation();
		int chunkNumber = readRequest.getChunkNumber();
		
		try {
			// make sure that the file is stored on a chunkserver first
			//if (filesOnChunkServersMap.containsKey(filename)) {
			if (filesWithChunksNodeInformationMap.containsKey(filename)) {
				// get the metadata for the chunkservers that hold the first value of the file
				// get the chunk servers associated with the file
				ArrayList<NodeInformation> chunkServers = filesWithChunksNodeInformationMap.get(filename).get(chunkNumber);
				int totalNumberOfFiles = filesWithChunksNodeInformationMap.get(filename).size();
				
				//ArrayList<HeartbeatMetadata> HeartbeatMetadataList = filesOnChunkServersMap.get(filename).get(chunkNumber);
				
				// get the first chunk server stored and try that one
				if (!chunkServers.isEmpty()) {
					ControllerChunkServerToReadResponseToClient controllerReponse = new ControllerChunkServerToReadResponseToClient(chunkServers.get(0), chunkNumber, filename, totalNumberOfFiles);
					this.clientNodesMap.get(clientNode).sendData(controllerReponse.getBytes());
				} else {
					System.out.println("No ChunkServers available to read the file from.");
				
				//int totalNumberOfFiles = filesOnChunkServersMap.get(filename).values().stream().mapToInt(List::size).sum();
				
				// get the first metadata information stored for the file and try that one
				/**
				if (!HeartbeatMetadataList.isEmpty()) {
					NodeInformation chunkServer = HeartbeatMetadataList.get(0).getNodeInformation();
					
					// put the chunkserver nodeinformation into a message and send to client
					ControllerChunkServerToReadResponseToClient controllerReponse = new ControllerChunkServerToReadResponseToClient(chunkServer, chunkNumber, filename, totalNumberOfFiles);
					
					this.clientNodesMap.get(clientNode).sendData(controllerReponse.getBytes());
					
				} else {
				**/
					//System.out.println("No ChunkServers available to read the file from.");
				}
			} else {
				System.out.println("File is not stored on any server.");
			}
		} catch  (IOException ioe) {
			ioe.printStackTrace();
		} 
		if (DEBUG) { System.out.println("end Controller handleClientReadRequest"); }
	}
	
	private void handleChunkServerSendMajorHeartbeatToController(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleChunkServerSendMajorHeartbeatToController"); }
		
		ChunkServerSendMajorHeartbeatToController majorHeartbeat = (ChunkServerSendMajorHeartbeatToController) event;
		
		NodeInformation chunkServer = majorHeartbeat.getChunkServer();
		int totalNumberOfChunks = majorHeartbeat.getTotalNumberOfChunks();
		long freespaceAvailable = majorHeartbeat.getFreespaceAvailable();
		ArrayList<Metadata> metadataList = majorHeartbeat.getMetadataList();
		
		HeartbeatMetadata hbm = new HeartbeatMetadata(chunkServer, totalNumberOfChunks, freespaceAvailable);
		hbm.setMetadata(metadataList);
		
		synchronized (chunkServerHeartbeatMetadaList) {
			chunkServerHeartbeatMetadaList.put(chunkServer, hbm);
		}
		
		if (DEBUG) { System.out.println("end Controller handleChunkServerSendMajorHeartbeatToController"); }
	}
	
	private void handleChunkServerSendMinorHeartbeatToController(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleChunkServerSendMinorHeartbeatToController"); }
		
		ChunkServerSendMinorHeartbeatToController minorHeartbeat = (ChunkServerSendMinorHeartbeatToController) event;
		
		NodeInformation chunkServer = minorHeartbeat.getChunkServer();
		int totalNumberOfChunks = minorHeartbeat.getTotalNumberOfChunks();
		long freespaceAvailable = minorHeartbeat.getFreespaceAvailable();
		ArrayList<Metadata> metadataList = minorHeartbeat.getMetadataList();
		
		HeartbeatMetadata hbm = new HeartbeatMetadata(chunkServer, totalNumberOfChunks, freespaceAvailable);

		synchronized (chunkServerHeartbeatMetadaList) {
			HeartbeatMetadata storedHBM = chunkServerHeartbeatMetadaList.get(chunkServer);
			ArrayList<Metadata> storedHBMMetadataList = storedHBM.getMetadataList();
			
			for (Metadata md : storedHBMMetadataList) {
				metadataList.add(md);
			}
			
			hbm.setMetadata(metadataList);
			chunkServerHeartbeatMetadaList.put(chunkServer, hbm);
		}
		
		if (DEBUG) { System.out.println("end Controller handleChunkServerSendMinorHeartbeatToController"); }
	}
	
	private void handleChunkServerDeletedChunkToController(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleChunkServerDeletedChunkToController"); }
		
		ChunkServerDeletedChunkToController deletedChunk = (ChunkServerDeletedChunkToController) event;
		
		NodeInformation corruptChunkServer = deletedChunk.getChunkServer();
		int chunknumber = deletedChunk.getChunkNumber();
		String filename = deletedChunk.getFilename();
		
		synchronized (filesWithChunksNodeInformationMap) {
			if (filesWithChunksNodeInformationMap.containsKey(filename)) {
				HashMap<Integer, ArrayList<NodeInformation>> tempMap = filesWithChunksNodeInformationMap.get(filename);
				if (tempMap.containsKey(chunknumber)) {
					ArrayList<NodeInformation> chunkServers = tempMap.get(chunknumber);
					
					if (!chunkServers.isEmpty()) {
						if (chunkServers.contains(corruptChunkServer)) {
							chunkServers.remove(corruptChunkServer);
						}
						if (!chunkServers.isEmpty()) {
							try {
								NodeInformation activeChunkServer = chunkServers.get(0);
								// update our map with the removed chunk server so it's not sent to again for this chunk number on this filename
								// adding back the corrupt chunk server for now since forwarding data should be considered successful
								chunkServers.add(corruptChunkServer);
								tempMap.put(chunknumber, chunkServers);
								filesWithChunksNodeInformationMap.put(filename, tempMap);

								ControllerForwardDataToNewChunkServer forwardData = new ControllerForwardDataToNewChunkServer(corruptChunkServer, chunknumber, filename);
								
								this.chunkServerNodesMap.get(activeChunkServer).sendData(forwardData.getBytes());
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							System.out.println("No Chunk Servers available after removing corrupt chunk server.");
						}
					} else {
						System.out.println("No Chunk Servers available at all.");
					}
				} else {
					System.out.println("File does not have a chunk number that exists on any known chunk server.");
				}
			} else {
				System.out.println("File does not exist on any known chunk server.");
			}
		}
		
		if (DEBUG) { System.out.println("end Controller handleChunkServerDeletedChunkToController"); }
	}
	
	private void handleChunkServerSendCorruptChunkToController(Event event) {
		if (DEBUG) { System.out.println("begin Controller handleChunkServerSendCorruptChunkToController"); }
		
		ChunkServerSendCorruptChunkToController sendCorruptChunk = (ChunkServerSendCorruptChunkToController) event;
		
		String filename = sendCorruptChunk.getFilename();
		int chunknumber = sendCorruptChunk.getChunknumber();
		NodeInformation corruptChunkServer = sendCorruptChunk.getChunkServer();
		
		synchronized (filesWithChunksNodeInformationMap) {
			if (filesWithChunksNodeInformationMap.containsKey(filename)) {
				HashMap<Integer, ArrayList<NodeInformation>> tempMap = filesWithChunksNodeInformationMap.get(filename);
				if (tempMap.containsKey(chunknumber)) {
					ArrayList<NodeInformation> chunkServers = tempMap.get(chunknumber);
					
					if (!chunkServers.isEmpty()) {
						if (chunkServers.contains(corruptChunkServer)) {
							chunkServers.remove(corruptChunkServer);
						}
						if (!chunkServers.isEmpty()) {
							try {
								NodeInformation activeChunkServer = chunkServers.get(0);
								// update our map with the removed chunk server so it's not sent to again for this chunk number on this filename
								tempMap.put(chunknumber, chunkServers);
								filesWithChunksNodeInformationMap.put(filename, tempMap);

								NodeInformation client = sendCorruptChunk.getClient();
								int numberOfBadSlices = sendCorruptChunk.getNumberOfBadSlices();
								ArrayList<Integer> badSlices = sendCorruptChunk.getBadSlices();
								int totalnumberofchunks = sendCorruptChunk.getTotalNumberOfChunks();
								
								ControllerForwardFixCorruptChunkToChunkServer fixCorrupt = new ControllerForwardFixCorruptChunkToChunkServer(corruptChunkServer, client, chunknumber, filename, numberOfBadSlices, badSlices, totalnumberofchunks);
								
								this.chunkServerNodesMap.get(activeChunkServer).sendData(fixCorrupt.getBytes());
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							System.out.println("No Chunk Servers available after removing corrupt chunk server.");
						}
					} else {
						System.out.println("No Chunk Servers available at all.");
					}
				} else {
					System.out.println("File does not have a chunk number that exists on any known chunk server.");
				}
			} else {
				System.out.println("File does not exist on any known chunk server.");
			}
		}
		
		if (DEBUG) { System.out.println("end Controller handleChunkServerSendCorruptChunkToController"); }
	}
	
	public void updateDeadChunkServers() {
		synchronized (this.chunkServerHeartbeatMetadaList) {
			if (!deadChunkServers.isEmpty()) {
				for (NodeInformation deadChunkServer : deadChunkServers) {
					// find the files that are associated with the dead chunk server
					
					synchronized (filesWithChunksNodeInformationMap) {
						for (String filename : filesWithChunksNodeInformationMap.keySet()) {
							
							HashMap<Integer, ArrayList<NodeInformation>> fileentry = filesWithChunksNodeInformationMap.get(filename);
							for (int chunkNumber : fileentry.keySet()) {
								ArrayList<NodeInformation> chunkServers = fileentry.get(chunkNumber);
								if (chunkServers.contains(deadChunkServer)) {
									chunkServers.remove(deadChunkServer);
									
									ArrayList<HeartbeatMetadata> tempHbmList = new ArrayList<HeartbeatMetadata>();
									for (HeartbeatMetadata hbm : this.chunkServerHeartbeatMetadaList.values()) {
										tempHbmList.add(hbm);
									}
									tempHbmList.sort((h1, h2) -> Long.compare(h2.getFreeSpaceAvailable(), h1.getFreeSpaceAvailable()));
									
									NodeInformation newChunkServer = null;
									
									// get the first chunk server available that is not currently in the list
									for (int i = 0; i < tempHbmList.size(); i++) {
										if (!chunkServers.contains(tempHbmList.get(i).getNodeInformation())) {
											newChunkServer = tempHbmList.get(i).getNodeInformation();
											break;
										}
									}
									
									if (newChunkServer != null) {
										// get another active chunk server to update this node information provided it is not also currently one of our potential dead chunk servers
										for (NodeInformation ni : chunkServers) {
											if (!deadChunkServers.contains(ni)) {
												try {
													NodeInformation activeChunkServer = ni;
													
													// update local information with new data that will be stored on this chunk server
													HashMap<Integer, ArrayList<NodeInformation>> tempMap = new HashMap<Integer, ArrayList<NodeInformation>>();
													chunkServers.add(newChunkServer);
													tempMap.put(chunkNumber, chunkServers);
													filesWithChunksNodeInformationMap.put(filename, tempMap);
													
													ControllerForwardDataToNewChunkServer forwardData = new ControllerForwardDataToNewChunkServer(newChunkServer, chunkNumber, filename);
													
													this.chunkServerNodesMap.get(activeChunkServer).sendData(forwardData.getBytes());
												} catch (IOException e) {
													e.printStackTrace();
												}
											}
										}
									}
								}
							}
						}
					}
					this.chunkServerHeartbeatMetadaList.remove(deadChunkServer);
					synchronized (chunkServerNodesMap) {
						this.chunkServerNodesMap.remove(deadChunkServer);
					}
				}
			}
		}
	}
	
	private void handleChunkServerNotifyFixSuccessToController(Event event) {
		
		ChunkServerNotifyFixSuccessToController fixSuccess = (ChunkServerNotifyFixSuccessToController) event;
		
		NodeInformation chunkServer = fixSuccess.getChunkServer();
		int chunknumber = fixSuccess.getChunkNumber();
		String filename = fixSuccess.getFilename();
		
		synchronized (filesWithChunksNodeInformationMap) {
			if (filesWithChunksNodeInformationMap.containsKey(filename)) {
				HashMap<Integer, ArrayList<NodeInformation>> tempMap = filesWithChunksNodeInformationMap.get(filename);
				if (tempMap.containsKey(chunknumber)) {
					ArrayList<NodeInformation> chunkServers = tempMap.get(chunknumber);
					
					chunkServers.add(chunkServer);
					tempMap.put(chunknumber, chunkServers);
					filesWithChunksNodeInformationMap.put(filename, tempMap);
				} else {
					ArrayList<NodeInformation> chunkServers = new ArrayList<NodeInformation>();
					chunkServers.add(chunkServer);
					tempMap.put(chunknumber, chunkServers);
					filesWithChunksNodeInformationMap.put(filename, tempMap);
				}
			} else {
				HashMap<Integer, ArrayList<NodeInformation>> tempMap = new HashMap<Integer, ArrayList<NodeInformation>>();
				ArrayList<NodeInformation> chunkServers = new ArrayList<NodeInformation>();
				chunkServers.add(chunkServer);
				tempMap.put(chunknumber, chunkServers);
				filesWithChunksNodeInformationMap.put(filename, tempMap);
			}
		}
	}
}
