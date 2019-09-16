package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ControllerHeartbeatToChunkServer implements Event {

	private final int type = Protocol.CONTROLLER_HEARTBEAT_TO_CHUNKSERVER;
	
	public ControllerHeartbeatToChunkServer() {}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 */
	public ControllerHeartbeatToChunkServer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_HEARTBEAT_TO_CHUNKSERVER) {
			System.out.println("Invalid Message Type for ControllerChunkServersResponseToClient");
			return;
		}
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
