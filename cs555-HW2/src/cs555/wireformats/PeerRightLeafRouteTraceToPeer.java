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

public class PeerRightLeafRouteTraceToPeer implements Event {

	private final int type = Protocol.PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER;
	private String originNodeIdentifier;
	private int numberOfTraces;
	private ArrayList<String> traceIdentifierList;
	
	public PeerRightLeafRouteTraceToPeer(String originNodeIdentifier, int numberOfTraces,
			ArrayList<String> traceIdentifierList) {
		this.originNodeIdentifier = originNodeIdentifier;
		this.numberOfTraces = numberOfTraces;
		this.traceIdentifierList = traceIdentifierList;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * originNodeIdentifier
	 * numberOfTraces
	 * traceIdentifierList
	 * @throws IOException 
	 */
	public PeerRightLeafRouteTraceToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_RIGHT_LEAF_ROUTE_TRACE_TO_PEER) {
			System.out.println("Invalid Message Type for PeerRightLeafRouteTraceToPeer");
			return;
		}

		// originNodeIdentifier
		int originNodeIdentifierLength = din.readInt();
		byte[] originNodeIdentifierBytes = new byte[originNodeIdentifierLength];
		din.readFully(originNodeIdentifierBytes);
		this.originNodeIdentifier = new String(originNodeIdentifierBytes);
		
		// numberOfTraces
		int numberOfTraces = din.readInt();

		this.numberOfTraces = numberOfTraces;
				
		// traceIdentifierList
		// declare as size of the numberOfTraces that we are being passed
		this.traceIdentifierList = new ArrayList<>(this.numberOfTraces);
		
		for (int i=0; i < this.numberOfTraces; i++) {
			int traceIdentifierListLength = din.readInt();
			byte[] traceIdentifierListBytes = new byte[traceIdentifierListLength];
			din.readFully(traceIdentifierListBytes);
			this.traceIdentifierList.add(new String(traceIdentifierListBytes));
		}

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

		// originNodeIdentifier
		byte[] originNodeIdentifierBytes = this.originNodeIdentifier.getBytes();
		int originNodeIdentifierLength = originNodeIdentifierBytes.length;
		dout.writeInt(originNodeIdentifierLength);
		dout.write(originNodeIdentifierBytes);
		
		// numberOfTraces
		dout.writeInt(this.numberOfTraces);
		
		// traceIdentifierList
		for (String s : this.traceIdentifierList) {
			byte[] traceBytes = s.getBytes();
			int traceLength = traceBytes.length;
			dout.writeInt(traceLength);
			dout.write(traceBytes);
		}

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}
	
	public String getOriginNodeIdentifier() {
		return originNodeIdentifier;
	}
	
	public int getNumberOfTraces() {
		return numberOfTraces;
	}

	public ArrayList<String> getTraceIdentifierList() {
		return traceIdentifierList;
	}

}
