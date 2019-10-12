package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PeerLeftLeafUpdateCompleteToPeer implements Event {

	private final int type = Protocol.PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER;
	
	public PeerLeftLeafUpdateCompleteToPeer() {}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * @throws IOException 
	 */
	public PeerLeftLeafUpdateCompleteToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_LEFT_LEAF_UPDATE_COMPLETE_TO_PEER) {
			System.out.println("Invalid Message Type for PeerLeftLeafUpdateCompleteToPeer");
			return;
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
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

}
