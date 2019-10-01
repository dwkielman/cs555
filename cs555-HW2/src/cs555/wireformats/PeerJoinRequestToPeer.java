package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs555.util.TableEntry;

public class PeerJoinRequestToPeer implements Event {

	private final int type = Protocol.PEER_JOIN_REQUEST_TO_PEER;
	private TableEntry tableEntry;
	private int numberOfTraces;
	private ArrayList<String> traceList;
	private int hopCount;
	
	public PeerJoinRequestToPeer(TableEntry tableEntry, int numberOfTraces, ArrayList<String> traceList, int hopCount) {
		this.tableEntry = tableEntry;
		this.numberOfTraces = numberOfTraces;
		this.traceList = traceList;
		this.hopCount = hopCount;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * tableEntry
	 * numberOfTraces
	 * traceList
	 * hopCount
	 * @throws IOException 
	 */
	public PeerJoinRequestToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_JOIN_REQUEST_TO_PEER) {
			System.out.println("Invalid Message Type for PeerJoinRequestToPeer");
			return;
		}
		
		// tableEntry
		int tableEntryLength = din.readInt();
		byte[] tableEntryBytes = new byte[tableEntryLength];
		din.readFully(tableEntryBytes);
		
		this.tableEntry = new TableEntry(tableEntryBytes);

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
		
		// tableEntry
		byte[] tableEntryBytes = this.tableEntry.getBytes();
		int tableEntryLength = tableEntryBytes.length;
		dout.writeInt(tableEntryLength);
		dout.write(tableEntryBytes);

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
	
	public TableEntry getTableEntry() {
		return this.tableEntry;
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
