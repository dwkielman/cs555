package cs555.replication.util;

import java.util.Comparator;

public class HeartbeatMetadata {
	
	private NodeInformation nodeInformation;
	private int totalNumberOfChunks;
	private long freeSpaceAvailable;
	
	public HeartbeatMetadata(NodeInformation nodeInformation, int totalNumberOfChunks, long freeSpaceAvailable) {
		this.nodeInformation = nodeInformation;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.freeSpaceAvailable = freeSpaceAvailable;
	}
	
	public NodeInformation getNodeInformation() {
		return nodeInformation;
	}
	
	public void setNodeInformation(NodeInformation nodeInformation) {
		this.nodeInformation = nodeInformation;
	}
	
	public int getTotalNumberOfChunks() {
		return totalNumberOfChunks;
	}
	
	public void setTotalNumberOfChunks(int totalNumberOfChunks) {
		this.totalNumberOfChunks = totalNumberOfChunks;
	}
	
	public long getFreeSpaceAvailable() {
		return freeSpaceAvailable;
	}
	
	public void setFreeSpaceAvailable(long freeSpaceAvailable) {
		this.freeSpaceAvailable = freeSpaceAvailable;
	}
	
}
