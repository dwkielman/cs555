package cs555.wireformats;

/**
 * Interactions between the chunk server and the nodes, including prescribed wire-formats and their type set by an int.
 */

public class Protocol {
	
	public static final int DISCOVERY_REGISTER_RESPONSE_TO_PEER = 6000;
	public static final int DISCOVERY_SEND_RANDOM_NODE_TO_PEER = 6001;
	public static final int DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA = 6002;
	public static final int DISCOVERY_READ_REQUEST_RESPONSE_TO_STOREDATA = 6003;
	
	public static final int PEER_REGISTER_REQUEST_TO_DISCOVERY = 7000;
	public static final int PEER_JOIN_REQUEST_TO_PEER = 7001;
	public static final int PEER_FORWARD_JOIN_REQUEST_TO_PEER = 7002;
	public static final int PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER = 7003;
	public static final int PEER_UPDATE_LEFT_LEAF_PEER = 7004;
	public static final int PEER_UPDATE_RIGHT_LEAF_PEER = 7005;
	public static final int PEER_UPDATE_ROUTING_TABLE_TO_PEER = 7006;
	public static final int PEER_STORE_DESTINATION_TO_STORE_DATA = 7007;
	public static final int PEER_FORWARD_STORE_REQUEST_TO_PEER = 7008;
	public static final int PEER_SEND_FILE_TO_STORE_DATA = 7009;
	public static final int PEER_FORWARD_READ_REQUEST_TO_PEER = 7010;
	public static final int PEER_FORWARD_FILE_TO_PEER = 7011;
	public static final int PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER = 7012;
	public static final int PEER_RIGHT_LEAF_UPDATE_COMPLETE_TO_PEER = 7013;
	public static final int PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY = 7014;
	public static final int PEER_REMOVE_PEER_TO_DISCOVERY = 7015;
	public static final int PEER_REMOVE_TO_LEFT_LEAF_PEER = 7016;
	public static final int PEER_REMOVE_TO_RIGHT_LEAF_PEER = 7017;
	public static final int PEER_REMOVE_FROM_ROUTING_TABLE_TO_PEER = 7018;
	public static final int PEER_LEFT_LEAF_ROUTE_TRACE_TO_PEER = 7019;
	public static final int PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER = 7020;
	
	public static final int STOREDATA_STORE_REQUEST_TO_DISCOVERY = 8000;
	public static final int STOREDATA_SEND_STORE_REQUEST_TO_PEER = 8001;
	public static final int STOREDATA_SEND_FILE_TO_PEER = 8002;
	public static final int STOREDATA_READ_REQUEST_TO_DISCOVERY = 8003;
	public static final int STOREDATA_SEND_READ_REQUEST_TO_PEER = 8004;
	
}
