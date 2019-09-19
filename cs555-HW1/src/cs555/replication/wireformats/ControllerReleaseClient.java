package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ControllerReleaseClient implements Event {
	
	private final int type = Protocol.CONTROLLER_RELEASE_CLIENT;
	private boolean access;
	
	public ControllerReleaseClient(boolean access) {
		this.access = access;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * boolean
	 */
	public ControllerReleaseClient(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_RELEASE_CLIENT) {
			System.out.println("Invalid Message Type for RegisterResponse");
			return;
		}
		
		// access
		this.access = din.readBoolean();

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
		
		// access
		dout.writeBoolean(this.access);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public boolean getAccess() {
		return this.access;
	}
}
