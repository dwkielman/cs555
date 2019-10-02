package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs555.util.RoutingTable;
import cs555.util.TableEntry;

public class PeerJoinRequestFoundDestinationToPeer implements Event {

	private final int type = Protocol.PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER;
	private TableEntry destinationTableEntry;
	private TableEntry leftLeafTableEntry;
	private TableEntry rightLeafTableEntry;
	private RoutingTable routingTable;
	private int numberOfTraces;
	private ArrayList<String> traceList;
	private int hopCount;
	
	public PeerJoinRequestFoundDestinationToPeer(TableEntry destinationTableEntry, TableEntry leftLeafTableEntry, TableEntry rightLeafTableEntry, RoutingTable routingTable, int numberOfTraces, ArrayList<String> traceList, int hopCount) {
		this.destinationTableEntry = destinationTableEntry;
		this.leftLeafTableEntry = leftLeafTableEntry;
		this.rightLeafTableEntry = rightLeafTableEntry;
		this.routingTable = routingTable;
		this.numberOfTraces = numberOfTraces;
		this.traceList = traceList;
		this.hopCount = hopCount;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * tableEntry
	 * leftLeafTableEntry
	 * rightLeafTableEntry
	 * routingTable
	 * numberOfTraces
	 * traceList
	 * hopCount
	 * @throws IOException 
	 */
	public PeerJoinRequestFoundDestinationToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_JOIN_REQUEST_FOUND_DESTINATION_TO_PEER) {
			System.out.println("Invalid Message Type for PeerJoinRequestFoundDestinationToPeer");
			return;
		}
		
		// destinationTableEntry
		int destinationTableEntryLength = din.readInt();
		byte[] destinationTableEntryBytes = new byte[destinationTableEntryLength];
		din.readFully(destinationTableEntryBytes);
		
		this.destinationTableEntry = new TableEntry(destinationTableEntryBytes);
		
		// leftLeafTableEntry
		int leftLeafTableEntryLength = din.readInt();
		byte[] leftLeafTableEntryBytes = new byte[leftLeafTableEntryLength];
		din.readFully(leftLeafTableEntryBytes);
		
		this.leftLeafTableEntry = new TableEntry(leftLeafTableEntryBytes);
				
		// rightLeafTableEntry
		int rightLeafTableEntryLength = din.readInt();
		byte[] rightLeafTableEntryBytes = new byte[rightLeafTableEntryLength];
		din.readFully(rightLeafTableEntryBytes);
		
		this.rightLeafTableEntry = new TableEntry(rightLeafTableEntryBytes);		

		// routingTable
		int routingTableLength = din.readInt();
		byte[] routingTableBytes = new byte[routingTableLength];
		din.readFully(routingTableBytes);
		
		this.routingTable = new RoutingTable(routingTableBytes);
		
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
		
		// destinationTableEntry
		byte[] destinationTableEntryBytes = this.destinationTableEntry.getBytes();
		int destinationTableEntryLength = destinationTableEntryBytes.length;
		dout.writeInt(destinationTableEntryLength);
		dout.write(destinationTableEntryBytes);

		// leftLeafTableEntry
		byte[] leftLeafTableEntryBytes = this.leftLeafTableEntry.getBytes();
		int leftLeafTableEntryLength = leftLeafTableEntryBytes.length;
		dout.writeInt(leftLeafTableEntryLength);
		dout.write(leftLeafTableEntryBytes);
		
		// rightLeafTableEntry
		byte[] rightLeafTableEntryBytes = this.rightLeafTableEntry.getBytes();
		int rightLeafTableEntryLength = rightLeafTableEntryBytes.length;
		dout.writeInt(rightLeafTableEntryLength);
		dout.write(rightLeafTableEntryBytes);
		
		// routingTable
		byte[] routingTableBytes = this.routingTable.getBytes();
		int routingTableLength = routingTableBytes.length;
		dout.writeInt(routingTableLength);
		dout.write(routingTableBytes);
		
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
	
	public TableEntry getDestinationTableEntry() {
		return this.destinationTableEntry;
	}
	
	public RoutingTable getRoutingTable() {
		return this.routingTable;
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

	public TableEntry getLeftLeafTableEntry() {
		return leftLeafTableEntry;
	}

	public TableEntry getRightLeafTableEntry() {
		return rightLeafTableEntry;
	}

}
