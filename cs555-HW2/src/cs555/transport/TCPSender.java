package cs555.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Creates a TCPSender object easily sending data for a given socket
 */
public class TCPSender {

	private Socket socket;
	private DataOutputStream dout;
	
	public TCPSender(Socket socket) throws IOException {
		this.socket = socket;
		dout = new DataOutputStream(socket.getOutputStream());
	}
	
	// use synchronized here to avoid deadlock and ensure only one data is read at a time
	public synchronized void sendData(byte[] dataToSend) throws IOException {
		int dataLength = dataToSend.length;
		dout.writeInt(dataLength);
		dout.write(dataToSend, 0, dataLength);
		dout.flush();
	}
}
