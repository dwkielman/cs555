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
				// CHUNKSERVER_REGISTER_REQUEST = 6000
				case Protocol.CHUNKSERVER_REGISTER_REQUEST:
					event = new ChunkServerRegisterRequest(marshalledBytes);
					break;
				// CHUNKSERVER_REGISTER_RESPONSE = 6001
				case Protocol.CHUNKSERVER_REGISTER_RESPONSE:
					event = new ChunkServerRegisterResponse(marshalledBytes);
					break;
				// CONTROLLER_REGISTER_RESPONSE_CHUNKSERVER = 7000
				case Protocol.CONTROLLER_REGISTER_RESPONSE_CHUNKSERVER:
					event = new ControllerRegisterResponseChunkServer(marshalledBytes);
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
