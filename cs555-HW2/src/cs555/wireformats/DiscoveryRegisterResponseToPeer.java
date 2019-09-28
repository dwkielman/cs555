package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DiscoveryRegisterResponseToPeer implements Event {

	private final int type = Protocol.DISCOVERY_REGISTER_RESPONSE_TO_PEER;
	private byte statusCode;
	private String additionalInfo;
	
	public DiscoveryRegisterResponseToPeer(byte statusCode, String additionalInfo) {
		this.statusCode = statusCode;
		this.additionalInfo = additionalInfo;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * statusCode
	 * additionalInfo
	 * @throws IOException 
	 */
	public DiscoveryRegisterResponseToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.DISCOVERY_REGISTER_RESPONSE_TO_PEER) {
			System.out.println("Invalid Message Type for DiscoveryRegisterResponseToPeer");
			return;
		}
		
		// statusCode
		this.statusCode = din.readByte();
		
		int additionalInfoLength = din.readInt();
		byte[] additionalInfoBytes = new byte[additionalInfoLength];
		din.readFully(additionalInfoBytes);
		
		// additionalInfo
		this.additionalInfo = new String(additionalInfoBytes);

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
		
		// statusCode
		dout.writeByte(this.statusCode);
		
		// additionalInfo
		byte[] additionalInfoBytes = this.additionalInfo.getBytes();
		int additionalInfoLength = additionalInfoBytes.length;
		dout.writeInt(additionalInfoLength);
		dout.write(additionalInfoBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		return marshalledBytes;
	}

	public byte getStatusCode() {
		return statusCode;
	}

	public String getAdditionalInfo() {
		return additionalInfo;
	}

}
