package cs555.replication.transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import cs555.replication.node.Node;

/**
 * Creates a TCPServerThread object used for running a given node's incoming connections via the port and node
 */

public class TCPServerThread implements Runnable  {
	private static final boolean DEBUG = false;
	private Node node;
	private ServerSocket ourServerSocket;
	private String hostIPAddress;
	private int portNumber;
	private boolean isRunning;
	
	public TCPServerThread(int portNumber, Node node) {
		try {
			//Create the server socket
			ourServerSocket = new ServerSocket(portNumber);
			this.portNumber = ourServerSocket.getLocalPort();
			this.node = node;
			this.node.setLocalHostPortNumber(ourServerSocket.getLocalPort());
		} catch(IOException e) {
			System.out.println("TCPServerThread::creating_the_socket:: " + e);
			System.exit(1);
		}
	}
	
	public void run() {
		// turn on the Server
		this.isRunning = true;
		
		// get the current host IP address
		try {
			this.hostIPAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		
		if (DEBUG) { System.out.println("Node is now listening on IP: " + this.hostIPAddress + " Port: " + this.portNumber); }
		
		while (isRunning) {
			try {
				//Block on accepting connections. Once it has received a connection it will return a socket for us to use.
				Socket incomingConnectionSocket = ourServerSocket.accept();
				TCPReceiverThread tcpReceiverThread = new TCPReceiverThread(incomingConnectionSocket, this.node);
				new Thread(tcpReceiverThread).start();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("IOException in TCPServerThread");
	            System.exit(1);
			}
		}
	}
}