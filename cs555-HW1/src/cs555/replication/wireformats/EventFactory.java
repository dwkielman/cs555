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
			
			}
		} catch (IOException ioe) {
			System.out.println("EventFactory Exception");
			ioe.printStackTrace();
		}
		return event;
	}
	
}
