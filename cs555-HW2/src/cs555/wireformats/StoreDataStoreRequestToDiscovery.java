package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.util.NodeInformation;

public class StoreDataStoreRequestToDiscovery implements Event {

	private final int type = Protocol.STOREDATA_STORE_REQUEST_TO_DISCOVERY;
	private NodeInformation storeData;
	private String filename;
	private String key;
	
	public StoreDataStoreRequestToDiscovery(NodeInformation storeData, String filename, String key) {
		this.storeData = storeData;
		this.filename = filename;
		this.key = key;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * filename
	 * key
	 * storeData
	 * @throws IOException 
	 */
	public StoreDataStoreRequestToDiscovery(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.STOREDATA_STORE_REQUEST_TO_DISCOVERY) {
			System.out.println("Invalid Message Type for StoreDataStoreRequestToDiscovery");
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
}
