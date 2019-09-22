package cs555.erasure.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.erasure.util.NodeInformation;


public class ClientRequestToReadFromChunkServer implements Event {

	private final int type = Protocol.CLIENT_READ_REQUEST_TO_CHUNKSERVER;
	private NodeInformation clientNodeInformation;
	private int chunkNumber;
	private int shardNumber;
	private String fileName;
	private int totalNumberOfChunks;
	
	public ClientRequestToReadFromChunkServer(NodeInformation nodeInformation, int chunkNumber, String fileName, int totalNumberOfChunks, int shardNumber) {
		this.clientNodeInformation = nodeInformation;
		this.chunkNumber = chunkNumber;
		this.fileName = fileName;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.shardNumber = shardNumber;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * NodeInformation
	 * chunkNumber
	 * fileName
	 * totalNumberOfChunks
	 * shardNumber
	 * @throws IOException 
	 */
	public ClientRequestToReadFromChunkServer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CLIENT_READ_REQUEST_TO_CHUNKSERVER) {
			System.out.println("Invalid Message Type for ClientRequestToReadFromChunkServer");
			return;
		}
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.clientNodeInformation = new NodeInformation(nodeInformationBytes);
		
		// chunkNumber
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;
		
		// Filename
		int fileNameLength = din.readInt();
		byte[] fileNameBytes = new byte[fileNameLength];
		din.readFully(fileNameBytes);
		
		this.fileName = new String(fileNameBytes);
		
		// totalNumberOfChunks
		int totalNumberOfChunks = din.readInt();

		this.totalNumberOfChunks = totalNumberOfChunks;
		
		// shardNumber
		int shardNumber = din.readInt();

		this.shardNumber = shardNumber;
		
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
		byte[] nodeInformationBytes = this.clientNodeInformation.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		// chunkNumber
		dout.writeInt(chunkNumber);
		
		// Filename
		byte[] fileNameBytes = this.fileName.getBytes();
		int fileNameLength = fileNameBytes.length;
		dout.writeInt(fileNameLength);
		dout.write(fileNameBytes);
		
		// totalNumberOfChunks
		dout.writeInt(this.totalNumberOfChunks);
		
		// shardNumber
		dout.writeInt(this.shardNumber);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
	
	public int getChunkNumber() {
		return this.chunkNumber;
	}
	
	public NodeInformation getClientNodeInformation() {
		return this.clientNodeInformation;
	}

	public String getFilename() {
		return this.fileName;
	}
	
	public int getTotalNumberOfChunks() {
		return this.totalNumberOfChunks;
	}
	
	public int getShardNumber() {
		return this.shardNumber;
	}

}
