package cs555.wireformats;

/**
 * Interactions between the chunk server and the nodes, including prescribed wire-formats and their type set by an int.
 */

public class Protocol {
	
	public static final int DISCOVERY_REGISTER_RESPONSE_TO_PEER = 6000;
	public static final int DISCOVERY_SEND_RANDOM_NODE_TO_PEER = 6001;
	
	public static final int PEER_REGISTER_REQUEST_TO_DISCOVERY = 7000;
	public static final int PEER_JOIN_REQUEST_TO_PEER = 7001;
	public static final int PEER_FORWARD_JOIN_REQUEST_TO_PEER = 7002;
	public static final int PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER = 7003;
	
	/**
	public static final int CONTROLLER_REGISTER_RESPONSE_TO_CLIENT = 6001;
	public static final int CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT = 6002;
	public static final int CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT = 6003;
	public static final int CONTROLLER_HEARTBEAT_TO_CHUNKSERVER = 6004;
	public static final int CONTROLLER_FORWARD_DATA_TO_NEW_CHUNKSERVER = 6005;
	public static final int CONTROLLER_FORWARD_CORRUPT_CHUNK_TO_CHUNKSERVER = 6006;
	public static final int CONTROLLER_FORWARD_ONLY_CORRUPT_CHUNK_TO_CHUNKSERVER = 6007;
	public static final int CONTROLLER_RELEASE_CLIENT = 6008;
	
	public static final int CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER = 7000;
	public static final int CHUNKSERVER_SEND_CHUNK_TO_LAST_CHUNKSERVER = 7001;
	public static final int CHUNKSERVER_SEND_CHUNK_TO_CLIENT = 7002;
	public static final int CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER = 7003;
	public static final int CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER = 7004;
	public static final int CHUNKSERVER_SEND_CORRUPT_CHUNK_T0_CONTROLLER = 7005;
	public static final int CHUNKSERVER_FIX_CORRUPT_CHUNK_TO_CHUNKSERVER = 7006;
	public static final int CHUNKSERVER_DELETED_CHUNK_TO_CONTROLLER = 7007;
	public static final int CHUNKSERVER_NOTIFY_FIX_SUCCESS_TO_CONTROLLER = 7008;
	public static final int CHUNKSERVER_SEND_ONLY_CORRUPT_CHUNK_T0_CONTROLLER = 7009;
	
	public static final int CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8000;
	public static final int CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER = 8001;
	public static final int CLIENT_SEND_CHUNK_TO_CHUNKSERVER = 8002;
	public static final int CLIENT_READ_REQUEST_TO_CONTROLLER = 8003;
	public static final int CLIENT_READ_REQUEST_TO_CHUNKSERVER = 8004;

	public static final int IMPROVED_CHUNKSERVER_FIX_CORRUPT_CHUNK_TO_CHUNKSERVER = 7010;
	**/
	
}
