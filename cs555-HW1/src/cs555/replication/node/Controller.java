package cs555.replication.node;

import java.net.InetAddress;
import java.net.UnknownHostException;

import cs555.replication.transport.TCPServerThread;
import cs555.replication.wireformats.Event;

/**
 * A controller node for managing information about chunk servers and chunks within the
 * system. There will be only 1 instance of the controller node.
 *
 */

public class Controller implements Node {

	private static final boolean DEBUG = false;
	private int portNumber;
	private TCPServerThread tCPServerThread;
	private Thread thread;
	
	private Controller(int portNumber) {
		this.portNumber = portNumber;
		
		try {
			TCPServerThread registryServerThread = new TCPServerThread(this.portNumber, this);
			this.tCPServerThread = registryServerThread;
			this.thread = new Thread(this.tCPServerThread);
			this.thread.start();
			System.out.println("Registry TCPServerThread running.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		// requires 1 argument to initialize a controller
		if(args.length != 1) {
            System.out.println("Invalid Arguments. Must include a port number.");
            return;
        }
		
		int controllerPortNumber = 0;
		
		try {
			controllerPortNumber = Integer.parseInt(args[0]);
		} catch (NumberFormatException nfe) {
			System.out.println("Invalid argument. Argument must be a number.");
			nfe.printStackTrace();
		}
		
		Controller controller = new Controller(controllerPortNumber);
		
		String controllerIP = "";
		
        try{
        	controllerIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Controller is running at IP Address: " + controllerIP + " on Port Number: " + controller.portNumber);
        //handleUserInput(controller);
	}
	
	@Override
	public void onEvent(Event event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLocalHostPortNumber(int localPort) {
		// TODO Auto-generated method stub
		
	}

}
