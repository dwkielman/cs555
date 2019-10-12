package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs555.util.NodeInformation;

public class PeerStoreDestinationToStoreData implements Event {

	private final int type = Protocol.PEER_STORE_DESTINATION_TO_STORE_DATA;
	private String filename;
	private String key;
	private NodeInformation peer;
	private int numberOfTraces;
	private ArrayList<String> traceList;
	private int hopCount;
	
	public PeerStoreDestinationToStoreData(String filename, String key, NodeInformation peer, int numberOfTraces,
			ArrayList<String> traceList, int hopCount) {
		this.filename = filename;
		this.key = key;
		this.peer = peer;
		this.numberOfTraces = numberOfTraces;
		this.traceList = traceList;
		this.hopCount = hopCount;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * filename
	 * key
	 * storeData
	 * numberOfTraces
	 * traceList
	 * hopCount
	 * @throws IOException 
	 */
	public PeerStoreDestinationToStoreData(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_STORE_DESTINATION_TO_STORE_DATA) {
			System.out.println("Invalid Message Type for PeerStoreDestinationToStoreData");
			return;
		}
		
		// filename
		int filenameLength = din.readInt();
		byte[] filenameBytes = new byte[filenameLength];
		din.readFully(filenameBytes);
		
		this.filename = new String(filenameBytes);
		
		// key
		int keyLength = din.readInt();
		byte[] keyBytes = new byte[keyLength];
		din.readFully(keyBytes);
		
		this.key = new String(keyBytes);

		// peer
		int peerLength = din.readInt();
		byte[] peerBytes = new byte[peerLength];
		din.readFully(peerBytes);
		this.peer = new NodeInformation(peerBytes);
		
		// numberOfTraces
		int numberOfTraces = din.readInt();

		this.numberOfTraces = numberOfTraces;
				
		// numberOfTraces
		// declare as size of the numberOfTraces that we are being passed
		this.traceList = new ArrayList<>(this.numberOfTraces);
		
		for (int i=0; i < this.numberOfTraces; i++) {
			int traceLength = din.readInt();
			byte[] traceBytes = new byte[traceLength];
			din.readFully(traceBytes);
			this.traceList.add(new String(traceBytes));
		}
		
		// hopCount
		int hopCount = din.readInt();

		this.hopCount = hopCount;
		
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
		
		// filename
		byte[] filenameBytes = this.filename.getBytes();
		int filenameLength = filenameBytes.length;
		dout.writeInt(filenameLength);
		dout.write(filenameBytes);
		
		// key
		byte[] keyBytes = this.key.getBytes();
		int keyLength = keyBytes.length;
		dout.writeInt(keyLength);
		dout.write(keyBytes);

		// peer
		byte[] peerBytes = this.peer.getBytes();
		int peerLength = peerBytes.length;
		dout.writeInt(peerLength);
		dout.write(peerBytes);
		
		// numberOfTraces
		dout.writeInt(this.numberOfTraces);
		
		// traceList
		for (String s : this.traceList) {
			byte[] traceBytes = s.getBytes();
			int traceLength = traceBytes.length;
			dout.writeInt(traceLength);
			dout.write(traceBytes);
		}
		
		// hopCount
		dout.writeInt(this.hopCount);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public String getFilename() {
		return filename;
	}

	public String getKey() {
		return key;
	}
	
	public NodeInformation getPeer() {
		return peer;
	}
	
	public int getNumberOfTraces() {
		return numberOfTraces;
	}

	public ArrayList<String> getTraceList() {
		return traceList;
	}

	public int getHopCount() {
		return hopCount;
	}

}
