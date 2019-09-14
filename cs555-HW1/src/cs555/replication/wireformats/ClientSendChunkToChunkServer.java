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

public class ClientSendChunkToChunkServer implements Event {

	private final int type = Protocol.CLIENT_SEND_CHUNK_TO_CHUNKSERVER;
	int replicationLevel;
	private ArrayList<NodeInformation> chunkServersNodeInfoList;
	private byte[] chunkBytes;
	private int chunkNumber;
	private String filename;
	
	public ClientSendChunkToChunkServer(int replicationLevel, ArrayList<NodeInformation> chunkServersNodeInfoList, byte[] chunkBytes, int chunkNumber, String filename) {
		this.replicationLevel = replicationLevel;
		this.chunkServersNodeInfoList = chunkServersNodeInfoList;
		this.chunkBytes = chunkBytes;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * replicationLevel
	 * chunkServersNodeInfoList
	 * chunkBytes[]
	 * chunkNumber
	 * filename
	 * @throws IOException 
	 */
	public ClientSendChunkToChunkServer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CLIENT_SEND_CHUNK_TO_CHUNKSERVER) {
			System.out.println("Invalid Message Type for ClientSendChunkToChunkServer");
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
		
		// chunkBytes
		int chunkBytesLength = din.readInt();
		byte[] chunkBytes = new byte[chunkBytesLength];
		din.readFully(chunkBytes);
		this.chunkBytes = chunkBytes;
		
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
		
		// replicationLevel
		dout.writeInt(this.replicationLevel);
		
		// chunkServersNodeInfoList
		for (NodeInformation n : this.chunkServersNodeInfoList) {
			byte[] cSNIBytes = n.getBytes();
			int cSNILength = cSNIBytes.length;
			dout.writeInt(cSNILength);
			dout.write(cSNIBytes);
		}
		
		//chunkBytes
		int chunkBytesLength = this.chunkBytes.length;
		dout.writeInt(chunkBytesLength);
		dout.write(chunkBytes);
		
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

	public ArrayList<NodeInformation> getChunkServersNodeInfoList() {
		return this.chunkServersNodeInfoList;
	}

	public byte[] getChunkBytes() {
		return this.chunkBytes;
	}

	public int getChunkNumber() {
		return this.chunkNumber;
	}

	public String getFilename() {
		return this.filename;
	}

}
