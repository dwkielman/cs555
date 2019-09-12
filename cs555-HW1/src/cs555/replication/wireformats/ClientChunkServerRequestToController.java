package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.replication.util.NodeInformation;

public class ClientChunkServerRequestToController implements Event {

	private final int type = Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER;
	private NodeInformation clienNodeInformation;
	private int chunkNumber;
	private String fileName;
	
	public ClientChunkServerRequestToController(NodeInformation nodeInformation, int chunkNumber, String fileName) {
		this.clienNodeInformation = nodeInformation;
		this.chunkNumber = chunkNumber;
		this.fileName = fileName;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * NodeInformation
	 * chunkNumber
	 * fileName
	 * @throws IOException 
	 */
	public ClientChunkServerRequestToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CLIENT_CHUNKSERVER_REQUEST_TO_CONTROLLER) {
			System.out.println("Invalid Message Type for RegisterRequest");
			return;
		}
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.clienNodeInformation = new NodeInformation(nodeInformationBytes);
		
		// chunkNumber
		// numberOfPeerMessagingNodes
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;
		
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
		
		// NodeInformation
		byte[] nodeInformationBytes = this.clienNodeInformation.getBytes();
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
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
	
	public int getChunkNumber() {
		return chunkNumber;
	}
	
	public NodeInformation getClienNodeInformation() {
		return clienNodeInformation;
	}

	public String getFilename() {
		return fileName;
	}


}
