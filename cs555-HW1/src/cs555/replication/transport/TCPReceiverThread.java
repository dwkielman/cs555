package cs555.replication.transport;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import cs555.replication.node.Node;
import cs555.replication.wireformats.Event;
import cs555.replication.wireformats.EventFactory;

/**
 * Creates a TCPReceieverThread used for incoming connections for a passed socket and node
 */

public class TCPReceiverThread implements Runnable {

	private Socket socket;
	private DataInputStream din;
	private Node node;
	private EventFactory eventFactory;
	
	public TCPReceiverThread(Socket socket, Node node) throws IOException {
		this.node = node;
		this.socket = socket;
		this.eventFactory = EventFactory.getInstance();
		din = new DataInputStream(socket.getInputStream());
	}
	
	@Override
	public void run() {
		
		int dataLength;
		
		while (socket != null) {
			try {
				dataLength = din.readInt();
				byte[] data = new byte[dataLength];
				din.readFully(data, 0, dataLength);
				
				// Concstruct Event and notify node of event
				Event event = eventFactory.createEvent(data, this.node);
				this.node.onEvent(event);
				
			} catch (SocketException se) {
				//System.out.println("SocketException in TCPReceiverThread");
				System.out.println(se.getMessage());
				break;
			} catch (IOException ioe) {
				//System.out.println("IOException in TCPReceiverThread");
				System.out.println(ioe.getMessage());
				break;
			}
		}
	}

}
