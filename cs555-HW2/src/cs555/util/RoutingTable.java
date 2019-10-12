package cs555.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class RoutingTable {
	
	private final static int NUMBER_OF_ROWS = 4;
	private final static int HEX_DIGITS = 16;
	
	private String identifier;
	private HashMap<Integer, HashMap<String, TableEntry>> table;
	
	public RoutingTable(String identifier) {
		this.identifier = identifier.toUpperCase();
		this.table = new HashMap<Integer, HashMap<String, TableEntry>>();
	}

	public RoutingTable(String identifier, HashMap<Integer, HashMap<String, TableEntry>> table) {
		this.identifier = identifier.toUpperCase();
		this.table = table;
	}
	
	public RoutingTable(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		// identifier
		int identifierLength = din.readInt();
		byte[] identifierBytes = new byte[identifierLength];
		din.readFully(identifierBytes);
		this.identifier = new String(identifierBytes);
		
		// table
		int numberOfEntries = din.readInt();
		
		HashMap<Integer, HashMap<String, TableEntry>> table = new HashMap<Integer, HashMap<String, TableEntry>>();
		
		for (int i=0; i < numberOfEntries; i++) {
			int rowNumber = din.readInt();
			
			HashMap<String, TableEntry> row = new HashMap<String, TableEntry>();
			
			int numberOfColumns = din.readInt();
			
			for (int j=0; j < numberOfColumns; j++) {
				int entryLength = din.readInt();
				byte[] entryBytes = new byte[entryLength];
				din.readFully(entryBytes, 0, entryLength);
				
				String entry = new String(entryBytes);
				
				TableEntry te = null;
				int teLength = din.readInt();
				if (teLength > 0) {
					byte[] teBytes = new byte[teLength];
					din.readFully(teBytes, 0, teLength);
					te = new TableEntry(teBytes);
				}
				row.put(entry, te);
			}
			table.put(rowNumber, row);
		}
		
		this.table = table;
	}
	
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		// identifier
		byte[] identifierBytes = this.identifier.getBytes();
		int identifierLength = identifierBytes.length;
		dout.writeInt(identifierLength);
		dout.write(identifierBytes);
		
		// table
		HashMap<Integer, HashMap<String, TableEntry>> table = this.table;
		
		int numberOfEntries = table.size();
		dout.writeInt(numberOfEntries);
		
		for (Map.Entry<Integer, HashMap<String, TableEntry>> entrySet : this.table.entrySet()) {
			int rowNumber = entrySet.getKey();
			HashMap<String, TableEntry> row = entrySet.getValue();
			
			dout.writeInt(rowNumber);
			
			int numberOfColumns = row.size();
			dout.writeInt(numberOfColumns);
			
			for (Map.Entry<String, TableEntry> rowEntry : row.entrySet()) {
				String entry = rowEntry.getKey();
				TableEntry te = rowEntry.getValue();
				
				byte[] entryBytes = entry.getBytes();
				int entryLength = entryBytes.length;
				dout.writeInt(entryLength);
				if (entryLength > 0) {
					dout.write(entryBytes);
				}
				if (te == null) {
					dout.writeInt(0);
				} else {
					byte[] teBytes = te.getBytes();
					int teLength = teBytes.length;
					dout.writeInt(teLength);
					if (teLength > 0) {
						dout.write(teBytes);
					}
				}
			}
		}
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	public HashMap<String, TableEntry> getRoutingTableEntry(int index) {
		HashMap<String, TableEntry> returnTable = null;
		
		if (!table.isEmpty()) {
			returnTable = table.get(index);
		}
		
		return returnTable;
	}
	
	public void generateRoutingTable() {
		// with 16-bit identifiers we will have 4 rows
		for (int i=0; i < NUMBER_OF_ROWS; i++) {
			HashMap<String, TableEntry> row = new HashMap<String, TableEntry>();
			// table has as many rows as there are hexadecimal digits in the peer identifier
			for (int j=0; j < HEX_DIGITS; j++) {
				String nodeHandle = this.identifier.substring(0, i) + Integer.toHexString(j).toUpperCase();
				row.put(nodeHandle, null);
			}
			this.table.put(i, row);
		}
	}
	
	public int getSizeOfRoutingTable() {
		return this.table.size();
	}
	
	public HashMap<Integer, HashMap<String, TableEntry>> getTable() {
		return this.table;
	}
	
	public void printRoutingTable() {
		System.out.println("=======================================================================");
        System.out.println("==================== ROUTING TABLE FOR NODE: " + identifier + " =====================");
        System.out.println("=======================================================================");
        for (Entry<Integer, HashMap<String, TableEntry>> entrySet : table.entrySet()) {
            Integer key = entrySet.getKey();
            Map<String, TableEntry> value = entrySet.getValue();
            System.out.println("Entry Key: " + key + " : TableEntry Value: " + value);
        }
        System.out.println("=======================================================================");
	}
}
