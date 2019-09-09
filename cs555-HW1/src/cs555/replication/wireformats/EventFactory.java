package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import cs555.replication.node.Node;

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
				// CONTROLLER_REGISTER_RESPONSE_TO_CHUNKSERVER = 6000
				case Protocol.CONTROLLER_REGISTER_RESPONSE_TO_CHUNKSERVER:
					event = new ControllerRegisterResponseToChunkServer(marshalledBytes);
					break;
				// CONTROLLER_REGISTER_RESPONSE_TO_CLIENT = 6001
				case Protocol.CONTROLLER_REGISTER_RESPONSE_TO_CLIENT:
					event = new ControllerRegisterResponseToClient(marshalledBytes);
					break;
				// CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER = 7000
				case Protocol.CHUNKSERVER_REGISTER_REQUEST_TO_CONTROLLER:
					event = new ChunkServerRegisterRequestToController(marshalledBytes);
					break;
				// CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8000
				case Protocol.CLIENT_REGISTER_REQUEST_TO_CONTROLLER:
					event = new ClientRegisterRequestToController(marshalledBytes);
					break;
				// CLIENT_REGISTER_REQUEST_TO_CONTROLLER = 8001
				case Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER:
					event = new ClientChunkServerRequestToController(marshalledBytes);
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
