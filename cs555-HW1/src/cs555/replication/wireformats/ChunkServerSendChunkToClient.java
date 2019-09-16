package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ChunkServerSendChunkToClient implements Event {

	private final int type = Protocol.CHUNKSERVER_SEND_CHUNK_TO_CLIENT;
	private byte[] chunkBytes;
	private int chunkNumber;
	private String filename;
	private int totalNumberOfChunks;
	
	public ChunkServerSendChunkToClient(byte[] chunkBytes, int chunkNumber, String filename,  int totalNumberOfChunks) {
		this.chunkBytes = chunkBytes;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
		this.totalNumberOfChunks = totalNumberOfChunks;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkBytes[]
	 * chunkNumber
	 * filename
	 * totalNumberOfChunks
	 * @throws IOException 
	 */
	public ChunkServerSendChunkToClient(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CHUNKSERVER_SEND_CHUNK_TO_CLIENT) {
			System.out.println("Invalid Message Type for ChunkServerSendChunkToClient");
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

		// totalNumberOfChunks
		dout.writeInt(this.totalNumberOfChunks);
		
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
	
	public int getTotalNumberOfChunks() {
		return this.totalNumberOfChunks;
	}

}
