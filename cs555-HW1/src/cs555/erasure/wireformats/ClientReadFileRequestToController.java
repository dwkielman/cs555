package cs555.erasure.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.erasure.util.NodeInformation;

public class ClientReadFileRequestToController implements Event {

	private final int type = Protocol.CLIENT_READ_REQUEST_TO_CONTROLLER;
	private int chunkNumber;
	private NodeInformation clienNodeInformation;
	private String fileName;
	
	public ClientReadFileRequestToController(int chunkNumber, NodeInformation nodeInformation, String fileName) {
		this.chunkNumber = chunkNumber;
		this.clienNodeInformation = nodeInformation;
		this.fileName = fileName;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkNumber
	 * NodeInformation
	 * fileName
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
		
		// chunkNumber
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;
				
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
		dout.writeInt(this.chunkNumber);	
				
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
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
	
	public int getChunkNumber() {
		return this.chunkNumber;
	}
	
	public NodeInformation getClienNodeInformation() {
		return this.clienNodeInformation;
	}

	public String getFilename() {
		return this.fileName;
	}
}
