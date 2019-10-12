package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import cs555.node.Node;

/**
 * EventFactory is Singleton class that creates instances of Event type that is used to send messages
 */

public class EventFactory {

	private static final EventFactory eventFactory = new EventFactory();
	private static final boolean DEBUG = false;
	
	private EventFactory() {};
	
	public static EventFactory getInstance() {
		return eventFactory;
	}
	
	public synchronized Event createEvent(byte[] marshalledBytes, Node node) {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		Event event = null;
		try {
			int type = din.readInt();
			baInputStream.close();
			din.close();
			
			if (DEBUG) { System.out.println("Message Type being passed is: " + type); }
			
			switch(type) {
				// DISCOVERY_REGISTER_RESPONSE_TO_PEER = 6000
				case Protocol.DISCOVERY_REGISTER_RESPONSE_TO_PEER:
					event = new DiscoveryRegisterResponseToPeer(marshalledBytes);
					break;
				// DISCOVERY_SEND_RANDOM_NODE_TO_PEER = 6001
				case Protocol.DISCOVERY_SEND_RANDOM_NODE_TO_PEER:
					event = new DiscoverySendRandomNodeToPeer(marshalledBytes);
					break;
				// DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA = 6002
				case Protocol.DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA:
					event = new DiscoveryStoreRequestResponseToStoreData(marshalledBytes);
					break;
				// DISCOVERY_READ_REQUEST_RESPONSE_TO_STOREDATA = 6003
				case Protocol.DISCOVERY_READ_REQUEST_RESPONSE_TO_STOREDATA:
					event = new DiscoveryReadRequestResponseToStoreData(marshalledBytes);
					break;
				// PEER_REGISTER_REQUEST_TO_DISCOVERY = 7000
				case Protocol.PEER_REGISTER_REQUEST_TO_DISCOVERY:
					event = new PeerRegisterRequestToDiscovery(marshalledBytes);
					break;
				// PEER_JOIN_REQUEST_TO_PEER = 7001
				case Protocol.PEER_JOIN_REQUEST_TO_PEER:
					event = new PeerJoinRequestToPeer(marshalledBytes);
					break;
				// PEER_FORWARD_JOIN_REQUEST_TO_PEER = 7002
				case Protocol.PEER_FORWARD_JOIN_REQUEST_TO_PEER:
					event = new PeerForwardJoinRequestToPeer(marshalledBytes);
					break;
				// PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER = 7003
				case Protocol.PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER:
					event = new PeerJoinRequestFoundDestinationToPeer(marshalledBytes);
					break;
				// PEER_UPDATE_LEFT_LEAF_PEER = 7004
				case Protocol.PEER_UPDATE_LEFT_LEAF_PEER:
					event = new PeerUpdateLeftLeafPeerNode(marshalledBytes);
					break;
				// PEER_UPDATE_RIGHT_LEAF_PEER = 7005
				case Protocol.PEER_UPDATE_RIGHT_LEAF_PEER:
					event = new PeerUpdateRightLeafPeerNode(marshalledBytes);
					break;
				// PEER_UPDATE_ROUTING_TABLE_TO_PEER = 7006
				case Protocol.PEER_UPDATE_ROUTING_TABLE_TO_PEER:
					event = new PeerUpdateRoutingTableToPeerNode(marshalledBytes);
					break;
				// PEER_STORE_DESTINATION_TO_STORE_DATA = 7007
				case Protocol.PEER_STORE_DESTINATION_TO_STORE_DATA:
					event = new PeerStoreDestinationToStoreData(marshalledBytes);
					break;
				// PEER_FORWARD_STORE_REQUEST_TO_PEER = 7008
				case Protocol.PEER_FORWARD_STORE_REQUEST_TO_PEER:
					event = new PeerForwardStoreRequestToPeer(marshalledBytes);
					break;
				// PEER_SEND_FILE_TO_STORE_DATA = 7009
				case Protocol.PEER_SEND_FILE_TO_STORE_DATA:
					event = new PeerSendFileToStoreData(marshalledBytes);
					break;
				// PEER_FORWARD_READ_REQUEST_TO_PEER = 7010
				case Protocol.PEER_FORWARD_READ_REQUEST_TO_PEER:
					event = new PeerForwardReadRequestToPeer(marshalledBytes);
					break;
				// PEER_FORWARD_FILE_TO_PEER = 7011
				case Protocol.PEER_FORWARD_FILE_TO_PEER:
					event = new PeerForwardFileToPeer(marshalledBytes);
					break;
				// PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER = 7012
				case Protocol.PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER:
					event = new PeerLeftLeafUpdateCompleteToPeer(marshalledBytes);
					break;
				// PEER_RIGHT_LEAF_UPDATE_COMPLETE_TO_PEER = 7013
				case Protocol.PEER_RIGHT_LEAF_UPDATE_COMPLETE_TO_PEER:
					event = new PeerRightLeafUpdateCompleteToPeer(marshalledBytes);
					break;
				// PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY = 7014
				case Protocol.PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY:
					event = new PeerUpdateCompleteAndInitializedToDiscovery(marshalledBytes);
					break;
				// PEER_REMOVE_PEER_TO_DISCOVERY = 7015
				case Protocol.PEER_REMOVE_PEER_TO_DISCOVERY:
					event = new PeerRemovePeerToDiscovery(marshalledBytes);
					break;
				// PEER_REMOVE_TO_LEFT_LEAF_PEER = 7016
				case Protocol.PEER_REMOVE_TO_LEFT_LEAF_PEER:
					event = new PeerRemoveToLeftLeafPeer(marshalledBytes);
					break;
				// PEER_REMOVE_TO_RIGHT_LEAF_PEER = 7017
				case Protocol.PEER_REMOVE_TO_RIGHT_LEAF_PEER:
					event = new PeerRemoveToRightLeafPeer(marshalledBytes);
					break;
				// PEER_REMOVE_FROM_ROUTING_TABLE_TO_PEER = 7018
				case Protocol.PEER_REMOVE_FROM_ROUTING_TABLE_TO_PEER:
					event = new PeerRemoveFromRoutingTableToPeer(marshalledBytes);
					break;
				// PEER_LEFT_LEAF_ROUTE_TRACE_TO_PEER = 7019
				case Protocol.PEER_LEFT_LEAF_ROUTE_TRACE_TO_PEER:
					event = new PeerLeftLeafRouteTraceToPeer(marshalledBytes);
					break;
				// PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER = 7020
				case Protocol.PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER:
					event = new PeerRightLeafRouteTraceToPeer(marshalledBytes);
					break;
				// STOREDATA_STORE_REQUEST_TO_DISCOVERY = 8000
				case Protocol.STOREDATA_STORE_REQUEST_TO_DISCOVERY:
					event = new StoreDataStoreRequestToDiscovery(marshalledBytes);
					break;
				// STOREDATA_SEND_STORE_REQUEST_TO_PEER = 8001
				case Protocol.STOREDATA_SEND_STORE_REQUEST_TO_PEER:
					event = new StoreDataSendStoreRequestToPeer(marshalledBytes);
					break;
				// STOREDATA_SEND_FILE_TO_PEER = 8002
				case Protocol.STOREDATA_SEND_FILE_TO_PEER:
					event = new StoreDataSendFileToPeer(marshalledBytes);
					break;
				// STOREDATA_READ_REQUEST_TO_DISCOVERY = 8003
				case Protocol.STOREDATA_READ_REQUEST_TO_DISCOVERY:
					event = new StoreDataReadRequestToDiscovery(marshalledBytes);
					break;
				// STOREDATA_SEND_READ_REQUEST_TO_PEER = 8004
				case Protocol.STOREDATA_SEND_READ_REQUEST_TO_PEER:
					event = new StoreDataSendReadRequestToPeer(marshalledBytes);
					break;
				default:
					System.out.println("Invalid Message Type");
					return null;
			}
		} catch (IOException ioe) {
			System.out.println("EventFactory Exception");
			ioe.printStackTrace();
		}
		return event;
	}
	
}
