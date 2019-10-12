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

public class PeerForwardReadRequestToPeer implements Event {

	private final int type = Protocol.PEER_FORWARD_READ_REQUEST_TO_PEER;
	private String filename;
	private String key;
	private NodeInformation storeData;
	private int numberOfTraces;
	private ArrayList<String> traceList;
	private int hopCount;
	
	public PeerForwardReadRequestToPeer(String filename, String key, NodeInformation storeData, int numberOfTraces,
			ArrayList<String> traceList, int hopCount) {
		this.filename = filename;
		this.key = key;
		this.storeData = storeData;
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
	public PeerForwardReadRequestToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_FORWARD_READ_REQUEST_TO_PEER) {
			System.out.println("Invalid Message Type for PeerForwardReadRequestToPeer");
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

		// storeData
		int storeDataLength = din.readInt();
		byte[] storeDataBytes = new byte[storeDataLength];
		din.readFully(storeDataBytes);
		this.storeData = new NodeInformation(storeDataBytes);
		
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

		// storeData
		byte[] storeDataBytes = this.storeData.getBytes();
		int storeDataLength = storeDataBytes.length;
		dout.writeInt(storeDataLength);
		dout.write(storeDataBytes);
		
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
	
	public NodeInformation getStoreData() {
		return storeData;
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
