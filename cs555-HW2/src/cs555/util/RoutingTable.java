package cs555.util;

import java.util.HashMap;

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
	
}
