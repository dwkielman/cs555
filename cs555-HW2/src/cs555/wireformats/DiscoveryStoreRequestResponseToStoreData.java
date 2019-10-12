package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import cs555.util.NodeInformation;

public class DiscoveryStoreRequestResponseToStoreData implements Event {

	private final int type = Protocol.DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA;
	private NodeInformation peer;
	private String filename;
	private String key;
	
	public DiscoveryStoreRequestResponseToStoreData(String filename, String key, NodeInformation peer) {
		this.filename = filename;
		this.key = key;
		this.peer = peer;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * filename
	 * key
	 * peer
	 * @throws IOException 
	 */
	public DiscoveryStoreRequestResponseToStoreData(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.DISCOVERY_STORE_REQUEST_RESPONSE_TO_STOREDATA) {
			System.out.println("Invalid Message Type for DiscoveryStoreRequestResponseToStoreData");
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

}
