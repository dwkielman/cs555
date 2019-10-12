package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.util.TableEntry;

public class PeerUpdateCompleteAndInitializedToDiscovery implements Event {

	private final int type = Protocol.PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY;
	private TableEntry tableEntry;
	
	public PeerUpdateCompleteAndInitializedToDiscovery(TableEntry tableEntry) {
		this.tableEntry = tableEntry;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * tableEntry
	 * @throws IOException 
	 */
	public PeerUpdateCompleteAndInitializedToDiscovery(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_UPDATE_COMPLETE_AND_INITIALIZED_TO_DISCOVERY) {
			System.out.println("Invalid Message Type for PeerUpdateCompleteAndInitializedToDiscovery");
			return;
		}

		// tableEntry
		int tableEntryLength = din.readInt();
		byte[] tableEntryBytes = new byte[tableEntryLength];
		din.readFully(tableEntryBytes);
		
		this.tableEntry = new TableEntry(tableEntryBytes);
		
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
				
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public TableEntry getTableEntry() {
		return tableEntry;
	}

}
