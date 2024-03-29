package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.replication.util.NodeInformation;

public class ClientReadFileRequestToController implements Event {

	private final int type = Protocol.CLIENT_READ_REQUEST_TO_CONTROLLER;
	private NodeInformation clienNodeInformation;
	private String fileName;
	private int chunkNumber;
	
	public ClientReadFileRequestToController(NodeInformation nodeInformation, String fileName, int chunkNumber) {
		this.clienNodeInformation = nodeInformation;
		this.fileName = fileName;
		this.chunkNumber = chunkNumber;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * NodeInformation
	 * fileName
	 * chunkNumber
	 * @throws IOException 
	 */
	public ClientReadFileRequestToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CLIENT_READ_REQUEST_TO_CONTROLLER) {
			System.out.println("Invalid Message Type for ClientReadFileRequestToController");
			return;
		}
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.clienNodeInformation = new NodeInformation(nodeInformationBytes);
		
		// Filename
		int fileNameLength = din.readInt();
		byte[] fileNameBytes = new byte[fileNameLength];
		din.readFully(fileNameBytes);
		
		this.fileName = new String(fileNameBytes);
		
		// chunkNumber
		int chunkNumber = din.readInt();
		
		this.chunkNumber = chunkNumber;
		
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
		
		// Filename
		byte[] fileNameBytes = this.fileName.getBytes();
		int fileNameLength = fileNameBytes.length;
		dout.writeInt(fileNameLength);
		dout.write(fileNameBytes);
		
		// chunkNumber
		dout.writeInt(this.chunkNumber);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
	
	public NodeInformation getClienNodeInformation() {
		return this.clienNodeInformation;
	}

	public String getFilename() {
		return this.fileName;
	}

	public int getChunkNumber() {
		return this.chunkNumber;
	}
}
