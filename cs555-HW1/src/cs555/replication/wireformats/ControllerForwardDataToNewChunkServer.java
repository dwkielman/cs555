package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.replication.util.NodeInformation;

public class ControllerForwardDataToNewChunkServer implements Event {

	private final int type = Protocol.CONTROLLER_FORWARD_DATA_TO_NEW_CHUNKSERVER;
	private NodeInformation chunkServer;
	private int chunkNumber;
	private String filename;
	
	public ControllerForwardDataToNewChunkServer(NodeInformation chunkServer, int chunkNumber, String filename) {
		this.chunkServer = chunkServer;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkServer
	 * chunkNumber
	 * filename
	 * @throws IOException 
	 */
	public ControllerForwardDataToNewChunkServer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_FORWARD_DATA_TO_NEW_CHUNKSERVER) {
			System.out.println("Invalid Message Type for ControllerForwardDataToNewChunkServer");
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

}
