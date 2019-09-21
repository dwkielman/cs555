package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.replication.util.NodeInformation;

public class ControllerChunkServerToReadResponseToClient implements Event {

	private final int type = Protocol.CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT;
	private NodeInformation chunkServerNodeInformation;
	private int chunkNumber;
	private String fileName;
	private int totalNumberOfChunks;
	
	public ControllerChunkServerToReadResponseToClient(NodeInformation nodeInformation, int chunkNumber, String fileName, int totalNumberOfChunks) {
		this.chunkServerNodeInformation = nodeInformation;
		this.chunkNumber = chunkNumber;
		this.fileName = fileName;
		this.totalNumberOfChunks = totalNumberOfChunks;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * NodeInformation
	 * chunkNumber
	 * fileName
	 * totalNumberOfChunks
	 * @throws IOException 
	 */
	public ControllerChunkServerToReadResponseToClient(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT) {
			System.out.println("Invalid Message Type for ClientChunkServerRequestToController");
			return;
		}
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.chunkServerNodeInformation = new NodeInformation(nodeInformationBytes);
		
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
		byte[] nodeInformationBytes = this.chunkServerNodeInformation.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		// chunkNumber
		dout.writeInt(this.chunkNumber);
		
		// Filename
		byte[] fileNameBytes = this.fileName.getBytes();
		int fileNameLength = fileNameBytes.length;
		dout.writeInt(fileNameLength);
		dout.write(fileNameBytes);
		
		// totalNumberOfChunks
		dout.writeInt(this.totalNumberOfChunks);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
	
	public int getChunkNumber() {
		return this.chunkNumber;
	}
	
	public NodeInformation getChunkServerNodeInformation() {
		return this.chunkServerNodeInformation;
	}

	public String getFilename() {
		return this.fileName;
	}
	
	public int getTotalNumberOfChunks() {
		return this.totalNumberOfChunks;
	}

}
