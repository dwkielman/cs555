package cs555.erasure.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs555.erasure.util.Metadata;
import cs555.erasure.util.NodeInformation;

public class ChunkServerSendMajorHeartbeatToController implements Event {
	
	private final int type = Protocol.CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER;
	private NodeInformation chunkServer;
	private ArrayList<Metadata> metadataList;
	private int numberOfMetadataFiles;
	private int totalNumberOfChunks;
	private long freespaceAvailable;
	
	public ChunkServerSendMajorHeartbeatToController(NodeInformation chunkServer, ArrayList<Metadata> metadataList,
			int numberOfMetadataFiles, int totalNumberOfChunks, long freespaceAvailable) {
		this.chunkServer = chunkServer;
		this.metadataList = metadataList;
		this.numberOfMetadataFiles = numberOfMetadataFiles;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.freespaceAvailable = freespaceAvailable;
	}
	
	@Override
	public int getType() {
		return this.type;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkServer
	 * numberOfMetadatFiles
	 * metadataList
	 * totalNumberOfChunks
	 * freespaceAvailable
	 * @throws IOException 
	 */
	public ChunkServerSendMajorHeartbeatToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CHUNKSERVER_SEND_MAJOR_HEARTBEAT_T0_CONTROLLER) {
			System.out.println("Invalid Message Type for ChunkServerSendMajorHeartbeatToController");
			return;
		}
		
		// chunkServer
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.chunkServer = new NodeInformation(nodeInformationBytes);
		
		// numberOfMetadataFiles
		int numberOfMetadataFiles = din.readInt();

		this.numberOfMetadataFiles = numberOfMetadataFiles;

		// metadataList
		// declare as size of the number of metadata files that we are being passed
		this.metadataList = new ArrayList<>(this.numberOfMetadataFiles);
		
		for (int i=0; i < this.numberOfMetadataFiles; i++) {
			int metadataLength = din.readInt();
			byte[] metadataBytes = new byte[metadataLength];
			din.readFully(metadataBytes);
			this.metadataList.add(new Metadata(metadataBytes));
		}
		
		// chunkNumber
		int totalNumberOfChunks = din.readInt();
		this.totalNumberOfChunks = totalNumberOfChunks;

		// freespaceAvailable
		long freespaceAvailable = din.readLong();
		this.freespaceAvailable = freespaceAvailable;
				
		baInputStream.close();
		din.close();
	}
	
	
	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		// chunkServer
		byte[] nodeInformationBytes = this.chunkServer.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		// numberOfMetadataFiles
		dout.writeInt(this.numberOfMetadataFiles);
		
		// metadataList
		for (Metadata m : this.metadataList) {
			byte[] metadataBytes = m.getBytes();
			int metadataLength = metadataBytes.length;
			dout.writeInt(metadataLength);
			dout.write(metadataBytes);
		}
		
		// chunkNumber
		dout.writeInt(this.totalNumberOfChunks);

		// freespaceAvailable
		dout.writeLong(this.freespaceAvailable);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public NodeInformation getChunkServer() {
		return chunkServer;
	}

	public ArrayList<Metadata> getMetadataList() {
		return metadataList;
	}

	public int getNumberOfMetadataFiles() {
		return numberOfMetadataFiles;
	}

	public int getTotalNumberOfChunks() {
		return totalNumberOfChunks;
	}

	public long getFreespaceAvailable() {
		return freespaceAvailable;
	}
}
