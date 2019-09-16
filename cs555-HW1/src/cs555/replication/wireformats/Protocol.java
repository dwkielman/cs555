package cs555.replication.wireformats;

/**
 * Interactions between the chunk server and the nodes, including prescribed wire-formats and their type set by an int.
 */

public class Protocol {
	
	public static final int CONTROLLER_REGISTER_RESPONSE_TO_CHUNKSERVER = 6000;
	public static final int CONTROLLER_REGISTER_RESPONSE_TO_CLIENT = 6001;
	public static final int CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT = 6002;
	public static final int CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT = 6003;
	public static final int CONTROLLER_HEARTBEAT_TO_CHUNKSERVER = 6004;
	
	public static final int CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER = 7000;
	public static final int CHUNKSERVER_SEND_CHUNK_TO_LAST_CHUNKSERVER = 7001;
	public static final int CHUNKSERVER_SEND_CHUNK_TO_CLIENT = 7002;
	public static final int CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER = 7003;
	public static final int CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER = 7004;
	
	public static final int CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8000;
	public static final int CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER = 8001;
	public static final int CLIENT_SEND_CHUNK_TO_CHUNKSERVER = 8002;
	public static final int CLIENT_READ_REQUEST_TO_CONTROLLER = 8003;
	public static final int CLIENT_READ_REQUEST_TO_CHUNKSERVER = 8004;
	
	/**
	public static final int 
	
	
	
	public static final int MINOR_HEARTBEAT = 6002;
	public static final int MAJOR_HEARTBEAT = 6003;
	public static final int CHUNK_SERVER_SECURITY_REQUEST = 6004;
	public static final int CONTROLLER_SECURITY_RESPONSE = 6005;

	public static final int NODE_CONNECTION_REQUEST = 6011;
	public static final int NODE_CONNECTION_RESPONSE = 6012;
	**/
}
