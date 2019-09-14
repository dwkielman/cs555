package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs555.replication.util.NodeInformation;


public class ControllerChunkServersResponseToClient implements Event {

	private final int type = Protocol.CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT;
	private int replicationLevel;
	private ArrayList<NodeInformation> chunkServersNodeInfoList;
	private int chunkNumber;
	private String filename;
	private long timestamp;
	
	public ControllerChunkServersResponseToClient(int replicationLevel, ArrayList<NodeInformation> chunkServersNodeInfoList, int chunkNumber, String filename, long timestamp) {
		this.replicationLevel = replicationLevel;
		this.chunkServersNodeInfoList = chunkServersNodeInfoList;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
		this.timestamp = timestamp;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * replicationLevel
	 * chunkServersNodeInfoList
	 * chunkNumber
	 * filename
	 * timestamp
	 * @throws IOException 
	 */
	public ControllerChunkServersResponseToClient(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT) {
			System.out.println("Invalid Message Type for ControllerChunkServersResponseToClient");
			return;
		}
		
		// replicationLevel
		int replicationLevel = din.readInt();

		this.replicationLevel = replicationLevel;
				

		// chunkServersNodeInfoList
		// declare as size of the replicationLevel that we are being passed
		this.chunkServersNodeInfoList = new ArrayList<>(this.replicationLevel);
		
		for (int i=0; i < this.replicationLevel; i++) {
			int cSNIength = din.readInt();
			byte[] cSNIBytes = new byte[cSNIength];
			din.readFully(cSNIBytes);
			this.chunkServersNodeInfoList.add(new NodeInformation(cSNIBytes));
		}
		
		// chunkNumber
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;

		int filenameLength = din.readInt();
		byte[] filenameBytes = new byte[filenameLength];
		din.readFully(filenameBytes);
		
		// filename
		this.filename = new String(filenameBytes);
		
		// timestamp
		long timestamp = din.readLong();
		
		this.timestamp = timestamp;
		
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
		
		// replicationLevel
		dout.writeInt(this.replicationLevel);
		
		// chunkServersNodeInfoList
		for (NodeInformation n : this.chunkServersNodeInfoList) {
			byte[] cSNIBytes = n.getBytes();
			int cSNILength = cSNIBytes.length;
			dout.writeInt(cSNILength);
			dout.write(cSNIBytes);
		}
		
		// chunkNumber
		dout.writeInt(this.chunkNumber);

		// filename
		byte[] filenameBytes = this.filename.getBytes();
		int filenameLength = filenameBytes.length;
		dout.writeInt(filenameLength);
		dout.write(filenameBytes);
		
		// timestamp
		dout.writeLong(this.timestamp);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	public int getReplicationLevel() {
		return this.replicationLevel;
	}

	public ArrayList<NodeInformation> getChunkServersNodeInfoList() {
		return this.chunkServersNodeInfoList;
	}

	public int getChunkNumber() {
		return this.chunkNumber;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
}
