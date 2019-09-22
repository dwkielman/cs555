package cs555.erasure.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cs555.erasure.util.NodeInformation;
public class ControllerChunkServerToReadResponseToClient implements Event {

	private final int type = Protocol.CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT;
	private int shardNumberWithChunkServerSize;
	private HashMap<Integer, NodeInformation> shardNumberWithChunkServer;
	private int chunkNumber;
	private int totalNumberOfChunks;
	private String filename;
	
	public ControllerChunkServerToReadResponseToClient(int shardNumberWithChunkServerSize, HashMap<Integer, NodeInformation> shardNumberWithChunkServer, int chunkNumber, int totalNumberOfChunks, String filename) {
		this.shardNumberWithChunkServerSize = shardNumberWithChunkServerSize;
		this.shardNumberWithChunkServer = shardNumberWithChunkServer;
		this.chunkNumber = chunkNumber;
		this.totalNumberOfChunks = totalNumberOfChunks;
		this.filename = filename; 
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * shardNumberWithChunkServerSize
	 * shardNumberWithChunkServer
	 * chunkNumber
	 * totalNumberOfChunks
	 * filename
	 * @throws IOException 
	 */
	public ControllerChunkServerToReadResponseToClient(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_CHUNKSERVER_TO_READ_RESPONSE_TO_CLIENT) {
			System.out.println("Invalid Message Type for ControllerChunkServersResponseToClient");
			return;
		}
		
		// shardNumberWithChunkServerSize
		int shardNumberWithChunkServerSize = din.readInt();

		this.shardNumberWithChunkServerSize = shardNumberWithChunkServerSize;
				

		// shardNumberWithChunkServer
		this.shardNumberWithChunkServer = new HashMap<Integer, NodeInformation>();
		
		for (int i=0; i < this.shardNumberWithChunkServerSize; i++) {
			int shardNumber = din.readInt();
			
			int cSNIength = din.readInt();
			byte[] cSNIBytes = new byte[cSNIength];
			din.readFully(cSNIBytes);
			NodeInformation cs = new NodeInformation(cSNIBytes);
			this.shardNumberWithChunkServer.put(shardNumber, cs);
		}
		
		// chunkNumber
		int chunkNumber = din.readInt();
		this.chunkNumber = chunkNumber;
		
		// totalNumberOfChunks
		int totalNumberOfChunks = din.readInt();
		this.totalNumberOfChunks = totalNumberOfChunks;

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
		
		// numberOfChunkServers
		dout.writeInt(this.shardNumberWithChunkServerSize);
		
		// shardNumberWithChunkServer
		for (Map.Entry<Integer, NodeInformation> entrySet : this.shardNumberWithChunkServer.entrySet()) {
			int shardNumber = entrySet.getKey();
			dout.writeInt(shardNumber);
			NodeInformation cs = entrySet.getValue();
			byte[] cSNIBytes = cs.getBytes();
			int cSNILength = cSNIBytes.length;
			dout.writeInt(cSNILength);
			dout.write(cSNIBytes);
		}
		
		// chunkNumber
		dout.writeInt(this.chunkNumber);
				
		// totalNumberOfChunks
		dout.writeInt(this.totalNumberOfChunks);

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

	public int getTotalNumberOfChunks() {
		return this.totalNumberOfChunks;
	}

	public String getFilename() {
		return this.filename;
	}

	public int getShardNumberWithChunkServerSize() {
		return shardNumberWithChunkServerSize;
	}

	public HashMap<Integer, NodeInformation> getShardNumberWithChunkServer() {
		return shardNumberWithChunkServer;
	}

	public int getChunkNumber() {
		return chunkNumber;
	}
}
