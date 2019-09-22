package cs555.erasure.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ClientRegisterRequestToController implements Event {

	private final int type = Protocol.CLIENT_REGISTER_REQUEST_TO_CONTROLLER;
	private String IPAddress;
	private int portNumber;
	
	public ClientRegisterRequestToController(String IPAddress, int portNumber) {
		this.IPAddress = IPAddress;
		this.portNumber = portNumber;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * IPAddress
	 * portNumber
	 * @throws IOException 
	 */
	public ClientRegisterRequestToController(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CLIENT_REGISTER_REQUEST_TO_CONTROLLER) {
			System.out.println("Invalid Message Type for RegisterRequest");
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

}
