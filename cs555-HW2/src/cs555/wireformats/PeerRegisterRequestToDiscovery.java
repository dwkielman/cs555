package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PeerRegisterRequestToDiscovery implements Event {

	private final int type = Protocol.PEER_REGISTER_REQUEST_TO_DISCOVERY;
	private String IPAddress;
	private int portNumber;
	private long freeSpace;
	
	public PeerRegisterRequestToDiscovery(String IPAddress, int portNumber, long freeSpace) {
		this.IPAddress = IPAddress;
		this.portNumber = portNumber;
		this.freeSpace = freeSpace;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * IPAddress
	 * portNumber
	 * freeSpace
	 * @throws IOException 
	 */
	public PeerRegisterRequestToDiscovery(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.PEER_REGISTER_REQUEST_TO_DISCOVERY) {
			System.out.println("Invalid Message Type for PeerRegisterRequestToDiscovery");
			return;
		}
		
		// IPAddress
		int IPAddressLength = din.readInt();
		byte[] IPAddressBytes = new byte[IPAddressLength];
		din.readFully(IPAddressBytes);
		
		this.IPAddress = new String(IPAddressBytes);
		
		// portNumber
		int portNumber = din.readInt();

		this.portNumber = portNumber;
		
		// freeSpace
		long freeSpace = din.readLong();
		
		this.freeSpace = freeSpace;
		
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
		
		// IPAddress
		byte[] IPAddressBytes = this.IPAddress.getBytes();
		int IPAddressLength = IPAddressBytes.length;
		dout.writeInt(IPAddressLength);
		dout.write(IPAddressBytes);
		
		// portNumber
		dout.writeInt(this.portNumber);
		
		// freeSpace
		dout.writeLong(freeSpace);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public String getIPAddress() {
		return IPAddress;
	}

	public int getPortNumber() {
		return portNumber;
	}
	
	public long getFreeSpace() {
		return freeSpace;
	}

}
