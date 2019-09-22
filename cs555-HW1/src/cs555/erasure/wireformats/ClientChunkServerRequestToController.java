package cs555.erasure.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.erasure.util.NodeInformation;

public class ClientChunkServerRequestToController implements Event {

	private final int type = Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER;
	private int chunkNumber;
	private NodeInformation clientNodeInformation;
	private int totalNumberOfChunks;
	private String fileName;
	
	public ClientChunkServerRequestToController(int chunkNumber, NodeInformation clientNodeInformation, int totalNumberOfChunks, String fileName) {
		this.chunkNumber = chunkNumber;
		this.clientNodeInformation = clientNodeInformation;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.fileName = fileName;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkNumber
	 * NodeInformation
	 * totalNumberOfChunks
	 * fileName
	 * @throws IOException 
	 */
	public ClientChunkServerRequestToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER) {
			System.out.println("Invalid Message Type for ClientChunkServerRequestToController");
			return;
		}
		
		// chunkNumber
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.clientNodeInformation = new NodeInformation(nodeInformationBytes);
		
		// totalNumberOfChunks
		int totalNumberOfChunks = din.readInt();
		this.totalNumberOfChunks = totalNumberOfChunks;
		
		// Filename
		int fileNameLength = din.readInt();
		byte[] fileNameBytes = new byte[fileNameLength];
		din.readFully(fileNameBytes);
		
		this.fileName = new String(fileNameBytes);

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
		
		// chunkNumber
		dout.writeInt(chunkNumber);
		
		// NodeInformation
		byte[] nodeInformationBytes = this.clientNodeInformation.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		// totalNumberOfChunks
		dout.writeInt(totalNumberOfChunks);
		
		// Filename
		byte[] fileNameBytes = this.fileName.getBytes();
		int fileNameLength = fileNameBytes.length;
		dout.writeInt(fileNameLength);
		dout.write(fileNameBytes);

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

	public int getTotalNumberOfChunks() {
		return this.totalNumberOfChunks;
	}
	
	public String getFilename() {
		return this.fileName;
	}

}
