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

public class RoutingTable {
	
	private final static int NUMBER_OF_ROWS = 4;
	private final static int HEX_DIGITS = 16;
	
	private String identifer;
	private HashMap<Integer, HashMap<String, TableEntry>> table;
	
	public RoutingTable(String identifer) {
		this.identifer = identifer.toUpperCase();
		this.table = new HashMap<Integer, HashMap<String, TableEntry>>();
	}

	public RoutingTable(String identifer, HashMap<Integer, HashMap<String, TableEntry>> table) {
		this.identifer = identifer.toUpperCase();
		this.table = table;
	}
	
	public RoutingTable(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		// identifer
		int identiferLength = din.readInt();
		byte[] identiferBytes = new byte[identiferLength];
		din.readFully(identiferBytes);
		this.identifer = new String(identiferBytes);
		
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
		
		// identifer
		byte[] identiferBytes = this.identifer.getBytes();
		int identiferLength = identiferBytes.length;
		dout.writeInt(identiferLength);
		dout.write(identiferBytes);
		
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
			// table has as many rows as there are hexadecimal digits in the peer identifer
			for (int j=0; j < HEX_DIGITS; j++) {
				String nodeHandle = this.identifer.substring(0, i) + Integer.toHexString(j).toUpperCase();
				row.put(nodeHandle, null);
			}
			this.table.put(i, row);
		}
	}
	
	public int getSizeOfRoutingTable() {
		return this.table.size();
	}
}
