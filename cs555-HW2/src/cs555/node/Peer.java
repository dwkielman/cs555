package cs555.node;

import java.net.InetAddress;
import java.net.UnknownHostException;

import cs555.transport.TCPSender;
import cs555.transport.TCPServerThread;
import cs555.util.NodeInformation;
import cs555.wireformats.Event;

public class Peer implements Node {

	private static boolean DEBUG = false;
	private NodeInformation discoverNodeInformation;
	private String localHostIPAddress;
	private int localHostPortNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	private TCPSender discoverySender;
	private NodeInformation peerNodeInformation;
	private static Peer peer;
	
	private Peer(String controllerIPAddress, int controllerPortNumber, int peerPortNumber, String peerIdentifer) {
		this.discoverNodeInformation = new NodeInformation(controllerIPAddress, controllerPortNumber);
		
		try {
			TCPServerThread serverThread = new TCPServerThread(peerPortNumber, this);
			this.tCPServerThread = serverThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			
			if (DEBUG) { System.out.println("My server port number is: " + this.localHostPortNumber); }
			
			this.localHostIPAddress = InetAddress.getLocalHost().getCanonicalHostName();
			
			if (DEBUG) { System.out.println("My host IP Address is: " + this.localHostIPAddress); }
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		this.peerNodeInformation = new NodeInformation(this.localHostIPAddress, this.localHostPortNumber);
		// Once the initialization is complete, client should send a registration request to the controller.
		//connectToDiscovery();
	}
	
	public static void main(String[] args) {
			
		String discoveryIPAddress = args[0];
		int discoveryPortNumber = 0;
		int peerPortNumber = 0;
		String peerIdentifer = null;
		
		// requires 3 argument to initialize a peer
		if (args.length == 3) {
			peerIdentifer = generateIdentifer();
		}
		if (args.length == 4) {
			peerIdentifer = args[3];
        } else if (args.length != 3) {
        	System.out.println("Invalid Arguments. Must include a Discovery IP Address, Port Number, Peer's Port Number and an optional identifier.");
            return;
        }
		
		try {
			discoveryPortNumber = Integer.parseInt(args[1]);
			peerPortNumber = Integer.parseInt(args[2]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Arguments must be a number.");
			nfe.printStackTrace();
		}
		peer = new Peer(discoveryIPAddress, discoveryPortNumber, peerPortNumber, peerIdentifer);
		
	}
	
	private static String generateIdentifer() {
		Long l1 = System.nanoTime();
        String hex = Long.toHexString(l1);
        String randomID = hex.substring(hex.length() - 4);
        randomID = randomID.toUpperCase();
        return randomID;
	}

	@Override
	public void onEvent(Event event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLocalHostPortNumber(int portNumber) {
		this.localHostPortNumber = portNumber;
	}
}
