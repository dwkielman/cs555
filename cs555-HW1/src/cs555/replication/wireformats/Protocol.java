package cs555.replication.wireformats;

/**
 * Interactions between the chunk server and the nodes, including prescribed wire-formats and their type set by an int.
 */

public class Protocol {
	public static final int REGISTER_REQUEST = 6000;
	public static final int REGISTER_RESPONSE = 6001;
	public static final int MINOR_HEARTBEAT = 6002;
	public static final int MAJOR_HEARTBEAT = 6003;
	public static final int CHUNK_SERVER_SECURITY_REQUEST = 6004;
	public static final int CONTROLLER_SECURITY_RESPONSE = 6005;

	public static final int NODE_CONNECTION_REQUEST = 6011;
	public static final int NODE_CONNECTION_RESPONSE = 6012;
}
