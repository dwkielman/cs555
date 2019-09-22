package cs555.erasure.transport;

import java.util.ArrayList;

import cs555.erasure.node.ChunkServer;
import cs555.erasure.util.Metadata;
import cs555.erasure.util.NodeInformation;
import cs555.erasure.wireformats.ChunkServerSendMajorHeartbeatToController;
import cs555.erasure.wireformats.ChunkServerSendMinorHeartbeatToController;

public class TCPHeartbeat implements Runnable {

	private ChunkServer chunkServer;
	private NodeInformation chunkServerNodeInformation;
	private static final long TIME_TO_SLEEP = 30000;
	private int numberOfMinorsToSend;
	private static final boolean DEBUG = false;
	
	public TCPHeartbeat(ChunkServer chunkServer, NodeInformation chunkServerNodeInformation) {
		this.chunkServer = chunkServer;
		this.chunkServerNodeInformation = chunkServerNodeInformation;
		this.numberOfMinorsToSend = 0;
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(TIME_TO_SLEEP);
				this.numberOfMinorsToSend++;
				
				if ((this.numberOfMinorsToSend % 10) == 0) {
					// major heartbeat from now on
					if (DEBUG) {System.out.println("ChunkServer sending Major Heartbeat to Controller."); }
					
					this.numberOfMinorsToSend = 0;
					
					long freespace = chunkServer.getFreeSpaceAvailable();
					int numberOfChunks = chunkServer.getNumberOfChunksStored();
					ArrayList<Metadata> metadata = chunkServer.getFilesWithMetadataMap();
					int numberOfMetadataFiles = metadata.size();
					int totalNumberOfShards = chunkServer.getNumberOfShardsStored();
					
					ChunkServerSendMajorHeartbeatToController majorHeartbeat = new ChunkServerSendMajorHeartbeatToController(chunkServerNodeInformation, metadata, numberOfMetadataFiles, numberOfChunks, freespace, totalNumberOfShards);
					chunkServer.getChunkServerSender().sendData(majorHeartbeat.getBytes());
					
					
				} else {
					// minor heartbeats, 10 minors until 5 minutes have passed
					if (DEBUG) {System.out.println("ChunkServer sending Minor Heartbeat to Controller."); }
					
					ArrayList<Metadata> metadata = chunkServer.getNewFilesWithMetadataMap();
					long freespace = chunkServer.getFreeSpaceAvailable();
					int numberOfChunks = chunkServer.getNumberOfChunksStored();
					int numberOfMetadataFiles = metadata.size();
					int totalNumberOfShards = chunkServer.getNumberOfShardsStored();
					
					ChunkServerSendMinorHeartbeatToController minorHeartbeat = new ChunkServerSendMinorHeartbeatToController(chunkServerNodeInformation, metadata, numberOfMetadataFiles, numberOfChunks, freespace, totalNumberOfShards);
					chunkServer.getChunkServerSender().sendData(minorHeartbeat.getBytes());
					
					// after sending, clear the metadata
					chunkServer.clearNewMetadataList();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

}
