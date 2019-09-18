package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class ChunkServerFixCorruptChunkToChunkServer implements Event {

	private final int type = Protocol.CHUNKSERVER_FIX_CORRUPT_CHUNK_TO_CHUNKSERVER;
	private byte[] chunkBytes;
	private int chunkNumber;
	private String filename;
	private long timestamp;
	private int numberOfBadSlices;
	private ArrayList<Integer> badSlices;
	private int numberOfDataStored;
	
	public ChunkServerFixCorruptChunkToChunkServer(byte[] chunkBytes, int chunkNumber, String filename, long timestamp, int numberOfBadSlices, ArrayList<Integer> badSlices, int numberOfDataStored) {
		this.chunkBytes = chunkBytes;
		this.chunkNumber = chunkNumber;
		this.filename = filename;
		this.timestamp = timestamp;
		this.numberOfBadSlices = numberOfBadSlices;
		this.badSlices = badSlices;
		this.numberOfDataStored = numberOfDataStored;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkBytes[]
	 * chunkNumber
	 * filename
	 * timestamp
	 * numberOfBadSlices
	 * badSlices
	 * numberOfDataStored
	 * @throws IOException 
	 */
	public ChunkServerFixCorruptChunkToChunkServer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CHUNKSERVER_FIX_CORRUPT_CHUNK_TO_CHUNKSERVER) {
			System.out.println("Invalid Message Type for ChunkServerFixCorruptChunkToChunkServer");
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
		
		// timestamp
		long timestamp = din.readLong();
		
		this.timestamp = timestamp;
		
		// numberOfBadSlices
		int numberOfBadSlices = din.readInt();

		this.numberOfBadSlices = numberOfBadSlices;
		
		// badSlices
		// declare as size of the number of metadata files that we are being passed
		this.badSlices = new ArrayList<>(this.numberOfBadSlices);
		for (int i=0; i < this.numberOfBadSlices; i++) {
			int badslice = din.readInt();
			this.badSlices.add(badslice);
		}
		
		// numberOfDataStored
		int numberOfDataStored = din.readInt();
		this.numberOfDataStored = numberOfDataStored;
				
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
				
		// timestamp
		dout.writeLong(this.timestamp);
		
		// numberOfBadSlices
		dout.writeInt(this.numberOfBadSlices);
		
		// badSlices
		for (Integer i : this.badSlices) {
			dout.writeInt(i);
		}
		
		// numberOfDataStored
		dout.writeInt(this.numberOfDataStored);
		
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
	
	public long getTimestamp() {
		return this.timestamp;
	}
	public int getNumberOfBadSlices() {
		return numberOfBadSlices;
	}

	public ArrayList<Integer> getBadSlices() {
		return badSlices;
	}
	public int getNumberOfDataStored() {
		return numberOfDataStored;
	}
}
