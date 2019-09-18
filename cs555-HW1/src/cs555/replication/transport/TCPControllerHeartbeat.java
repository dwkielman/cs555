package cs555.replication.transport;

import java.net.Socket;
import java.util.Set;

import cs555.replication.node.Controller;
import cs555.replication.util.NodeInformation;
import cs555.replication.wireformats.ControllerHeartbeatToChunkServer;

public class TCPControllerHeartbeat implements Runnable {

	private Controller controller;
	//private static final long TIME_TO_SLEEP = 30000;
	private static final long TIME_TO_SLEEP = 10000;
	private static final boolean DEBUG = true;
	
	public TCPControllerHeartbeat(Controller controller) {
		this.controller = controller;
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(TIME_TO_SLEEP);
				
				Set<NodeInformation> chunkServersToCheck = this.controller.getLiveChunkServers();
				System.out.println("Controller Heartbeat Running");

				if (chunkServersToCheck != null) {
					if (!chunkServersToCheck.isEmpty()) {
						if (DEBUG) {System.out.println("Controller probing " + chunkServersToCheck.size() + " number of Chunk Servers."); }
						
						for (NodeInformation chunkServer : chunkServersToCheck) {
							try {
								Socket socket = new Socket(chunkServer.getNodeIPAddress(), chunkServer.getNodePortNumber());
								
								ControllerHeartbeatToChunkServer heartbeatToChunkServer = new ControllerHeartbeatToChunkServer();
								
								controller.getChunkServerSender(chunkServer).sendData(heartbeatToChunkServer.getBytes());
							} catch (Exception e) {
								System.out.println("ERROR: Chunk Server not responding. Adding to dead chunk servers in Controller.");
								controller.updateDeadChunkServers(chunkServer);
							}
						}
					} else {
						System.out.println("No Chunk Servers Running.");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				
			}
		}
	}
}