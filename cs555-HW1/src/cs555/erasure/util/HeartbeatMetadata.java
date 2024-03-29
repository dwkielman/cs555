package cs555.erasure.util;

import java.util.ArrayList;

public class HeartbeatMetadata {
	
	private NodeInformation nodeInformation;
	private int totalNumberOfChunks;
	private long freeSpaceAvailable;
	private ArrayList<Metadata> metadataList;
	private int totalNumberOfShards;
	
	public HeartbeatMetadata(NodeInformation nodeInformation, int totalNumberOfChunks, long freeSpaceAvailable, int totalNumberOfShards) {
		this.nodeInformation = nodeInformation;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.freeSpaceAvailable = freeSpaceAvailable;
		this.metadataList = new ArrayList<Metadata>();
		this.totalNumberOfShards = totalNumberOfShards;
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
	
	public void addMetadata(Metadata m) {
		this.metadataList.add(m);
	}
	
	public void setMetadata(ArrayList<Metadata> metadataList) {
		this.metadataList.clear();
		for (Metadata m : metadataList) {
			this.metadataList.add(m);
		}
	}
	
	public ArrayList<Metadata> getMetadataList() {
		return this.metadataList;
	}
	
	public int getTotalNumberOfShards() {
		return totalNumberOfShards;
	}
	
	public void setTotalNumberOfShards(int totalNumberOfShards) {
		this.totalNumberOfShards = totalNumberOfShards;
	}
}
