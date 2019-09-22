package cs555.erasure.wireformats;

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
	private byte[] chunkBytes;
	private int chunkNumber;
	private String filename;
	private int shardNumber;
	
	public ClientSendChunkToChunkServer(byte[] chunkBytes, int chunkNumber, String filename, int shardNumber) {
		this.chunkBytes = chunkBytes;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
		this.shardNumber = shardNumber;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkBytes[]
	 * chunkNumber
	 * filename
	 * shardNumber
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

		// shardNumber
		dout.writeInt(this.shardNumber);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();

		return marshalledBytes;
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
	
	public int getShardNumber() {
		return this.shardNumber;
	}
}
