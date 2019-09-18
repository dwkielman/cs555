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

public class ControllerForwardOnlyFixCorruptChunkToChunkServer implements Event {
	
	private final int type = Protocol.CONTROLLER_FORWARD_ONLY_CORRUPT_CHUNK_TO_CHUNKSERVER;
	private NodeInformation chunkServer;
	private int chunknumber;
	private String filename;
	private int numberOfBadSlices;
	private ArrayList<Integer> badSlices;
	
	public ControllerForwardOnlyFixCorruptChunkToChunkServer(NodeInformation chunkServer, int chunknumber,
			String filename, int numberOfBadSlices, ArrayList<Integer> badSlices) {
		super();
		this.chunkServer = chunkServer;
		this.chunknumber = chunknumber;
		this.filename = filename;
		this.numberOfBadSlices = numberOfBadSlices;
		this.badSlices = badSlices;
	}
	
	@Override
	public int getType() {
		return this.type;
	}

	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkServer
	 * chunknumber
	 * filename
	 * numberOfBadSlices
	 * badSlices
	 * @throws IOException 
	 */
	public ControllerForwardOnlyFixCorruptChunkToChunkServer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_FORWARD_ONLY_CORRUPT_CHUNK_TO_CHUNKSERVER) {
			System.out.println("Invalid Message Type for ControllerForwardOnlyFixCorruptChunkToChunkServer");
			return;
		}
		
		// chunkServer
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.chunkServer = new NodeInformation(nodeInformationBytes);
		
		// chunknumber
		int chunknumber = din.readInt();

		this.chunknumber = chunknumber;

		int filenameLength = din.readInt();
		byte[] filenameBytes = new byte[filenameLength];
		din.readFully(filenameBytes);
		
		// filename
		this.filename = new String(filenameBytes);
		
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
		
		baInputStream.close();
		din.close();
	}
	
	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		dout.writeInt(this.type);
		
		// chunkServer
		byte[] nodeInformationBytes = this.chunkServer.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);
		
		// chunknumber
		dout.writeInt(this.chunknumber);
		
		// filename
		byte[] filenameBytes = this.filename.getBytes();
		int filenameLength = filenameBytes.length;
		dout.writeInt(filenameLength);
		dout.write(filenameBytes);
		
		// numberOfBadSlices
		dout.writeInt(this.numberOfBadSlices);
		
		// badSlices
		for (Integer i : this.badSlices) {
			dout.writeInt(i);
		}
				
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public NodeInformation getChunkServer() {
		return chunkServer;
	}

	public int getChunknumber() {
		return chunknumber;
	}

	public String getFilename() {
		return filename;
	}

	public int getNumberOfBadSlices() {
		return numberOfBadSlices;
	}

	public ArrayList<Integer> getBadSlices() {
		return badSlices;
	}
}
