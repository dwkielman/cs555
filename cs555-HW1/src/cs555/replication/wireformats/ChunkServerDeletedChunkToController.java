package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.replication.util.NodeInformation;

public class ChunkServerDeletedChunkToController implements Event {

	private final int type = Protocol.CHUNKSERVER_DELETED_CHUNK_TO_CONTROLLER;
	private NodeInformation chunkServer;
	private int chunkNumber;
	private String filename;
	private NodeInformation client;
	private int totalNumberOfChunks;
	private boolean forwardChunkToClient;
	
	public ChunkServerDeletedChunkToController(NodeInformation chunkServer, int chunkNumber, String filename, NodeInformation client, int totalNumberOfChunks, boolean forwardChunkToClient) {
		this.chunkServer = chunkServer;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
		this.client = client;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.forwardChunkToClient = forwardChunkToClient;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkServer
	 * chunkNumber
	 * filename
	 * client
	 * totalNumberOfChunks
	 * forwardChunkToClient
	 * @throws IOException 
	 */
	public ChunkServerDeletedChunkToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CHUNKSERVER_DELETED_CHUNK_TO_CONTROLLER) {
			System.out.println("Invalid Message Type for ChunkServerDeletedChunkToController");
			return;
		}
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.chunkServer = new NodeInformation(nodeInformationBytes);
		
		// chunkNumber
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;

		int filenameLength = din.readInt();
		byte[] filenameBytes = new byte[filenameLength];
		din.readFully(filenameBytes);
		
		// filename
		this.filename = new String(filenameBytes);
		
		// NodeInformation
		int clientNodeInformationLength = din.readInt();
		byte[] clientNodeInformationBytes = new byte[clientNodeInformationLength];
		din.readFully(clientNodeInformationBytes);
		this.client = new NodeInformation(clientNodeInformationBytes);
		
		// totalNumberOfChunks
		int totalNumberOfChunks = din.readInt();
		this.totalNumberOfChunks = totalNumberOfChunks;
		
		// forwardChunkToClient
		this.forwardChunkToClient = din.readBoolean();
		
		baInputStream.close();
		din.close();
	}

	@Override
	public int getType() {
		return this.type;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		// NodeInformation
		byte[] nodeInformationBytes = this.chunkServer.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		// chunkNumber
		dout.writeInt(this.chunkNumber);

		// filename
		byte[] filenameBytes = this.filename.getBytes();
		int filenameLength = filenameBytes.length;
		dout.writeInt(filenameLength);
		dout.write(filenameBytes);
		
		// NodeInformation
		byte[] clientNodeInformationBytes = this.client.getBytes();
		int clientNodeInformationLength = clientNodeInformationBytes.length;
		dout.writeInt(clientNodeInformationLength);
		dout.write(clientNodeInformationBytes);
		
		// totalNumberOfChunks
		dout.writeInt(this.totalNumberOfChunks);
		
		// forwardChunkToClient
		dout.writeBoolean(this.forwardChunkToClient);
	
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public NodeInformation getChunkServer() {
		return this.chunkServer;
	}
	
	public int getChunkNumber() {
		return this.chunkNumber;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
	public NodeInformation getClient() {
		return this.client;
	}
	
	public int getTotalNumberOfChunks() {
		return this.totalNumberOfChunks;
	}
	
	public boolean getForwardChunkToClient() {
		return forwardChunkToClient;
	}
}
