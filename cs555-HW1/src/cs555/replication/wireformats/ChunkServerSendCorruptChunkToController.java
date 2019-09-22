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

public class ChunkServerSendCorruptChunkToController implements Event {
	
	private final int type = Protocol.CHUNKSERVER_SEND_CORRUPT_CHUNK_T0_CONTROLLER;
	private NodeInformation chunkServer;
	private NodeInformation client;
	private int chunknumber;
	private String filename;
	private int numberOfBadSlices;
	private ArrayList<Integer> badSlices;
	private int totalnumberofchunks;
	private boolean forwardChunkToClient;
	
	public ChunkServerSendCorruptChunkToController(NodeInformation chunkServer, NodeInformation client, int chunknumber,
			String filename, int numberOfBadSlices, ArrayList<Integer> badSlices, int totalnumberofchunks, boolean forwardChunkToClient) {
		super();
		this.chunkServer = chunkServer;
		this.client = client;
		this.chunknumber = chunknumber;
		this.filename = filename;
		this.numberOfBadSlices = numberOfBadSlices;
		this.badSlices = badSlices;
		this.totalnumberofchunks = totalnumberofchunks;
		this.forwardChunkToClient = forwardChunkToClient;
	}
	
	@Override
	public int getType() {
		return this.type;
	}

	/**
	 * byte[] construction is as follows:
	 * type
	 * chunkServer
	 * client
	 * chunknumber
	 * filename
	 * numberOfBadSlices
	 * badSlices
	 * totalnumberofchunks
	 * forwardChunkToClient
	 * @throws IOException 
	 */
	public ChunkServerSendCorruptChunkToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CHUNKSERVER_SEND_CORRUPT_CHUNK_T0_CONTROLLER) {
			System.out.println("Invalid Message Type for ChunkServerSendCorruptChunkToController");
			return;
		}
		
		// chunkServer
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.chunkServer = new NodeInformation(nodeInformationBytes);
		
		// clientNode
		int clientNodeInformationLength = din.readInt();
		byte[] clientNodeInformationBytes = new byte[clientNodeInformationLength];
		din.readFully(clientNodeInformationBytes);
		this.client = new NodeInformation(clientNodeInformationBytes);
		
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
		
		// totalnumberofchunks
		int totalnumberofchunks = din.readInt();

		this.totalnumberofchunks = totalnumberofchunks;
		
		// forwardChunkToClient
		this.forwardChunkToClient = din.readBoolean();
		
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
		
		// clientNode
		byte[] clientNodeInformationBytes = this.client.getBytes();
		int clientNodeInformationLength = clientNodeInformationBytes.length;
		dout.writeInt(clientNodeInformationLength);
		dout.write(clientNodeInformationBytes);
		
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
		
		// totalnumberofchunks
		dout.writeInt(this.totalnumberofchunks);
		
		// forwardChunkToClient
		dout.writeBoolean(this.forwardChunkToClient);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}

	public NodeInformation getChunkServer() {
		return chunkServer;
	}

	public NodeInformation getClient() {
		return client;
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
	
	public int getTotalNumberOfChunks() {
		return totalnumberofchunks;
	}

	public boolean getForwardChunkToClient() {
		return forwardChunkToClient;
	}
}
