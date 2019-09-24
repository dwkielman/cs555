package cs555.erasure.wireformats;

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
	public static final int CHUNKSERVER_SEND_CHUNK_TO_CLIENT = 7002;
	public static final int CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER = 7003;
	public static final int CHUNKSERVER_SEND_MINOR_HEARTBEAT_T0_CONTROLLER = 7004;
	
	public static final int CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8000;
	public static final int CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER = 8001;
	public static final int CLIENT_SEND_CHUNK_TO_CHUNKSERVER = 8002;
	public static final int CLIENT_READ_REQUEST_TO_CONTROLLER = 8003;
	public static final int CLIENT_READ_REQUEST_TO_CHUNKSERVER = 8004;
	
}
