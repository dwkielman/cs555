package cs555.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * NodeInformation is a way to capture relevant information for keeping track of information regarding MessagingNodes in the Registry and a way to override the Equals() method
 * as doing so in the MessagingNode class itself would wreak havoc as it would create countless Threads in attempting to compare Nodes that are already in the Registry. Also allows an easy way
 * to keep track of the information that will be needed to pull reports on.
 */

public class NodeInformation {

	private String nodeIPAddress;
	private int nodePortNumber;
	private int numberOfConnections;
	
	public NodeInformation(String nodeIPAddress, int nodePortNumber) {
		this.nodeIPAddress = nodeIPAddress;
		this.nodePortNumber = nodePortNumber;
		this.numberOfConnections = 0;
	}
	
	public NodeInformation(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		// IP Address, String
		int nodeIPAddressLength = din.readInt();
		byte[] nodeIPAddressBytes = new byte[nodeIPAddressLength];
		din.readFully(nodeIPAddressBytes);
		this.nodeIPAddress = new String(nodeIPAddressBytes);
		
		// port number, int
		int portNumber = din.readInt();
		this.nodePortNumber = portNumber;
		
		baInputStream.close();
		din.close();
	}
	
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		// IP Address, String
		byte[] nodeIPAddressBytes = this.nodeIPAddress.getBytes();
		int nodeIPAddressLength = nodeIPAddressBytes.length;
		dout.writeInt(nodeIPAddressLength);
		dout.write(nodeIPAddressBytes);
		
		// port number, int
		dout.writeInt(this.nodePortNumber);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof NodeInformation) {
			NodeInformation otherNodeInformation = (NodeInformation) o;
			if (otherNodeInformation.getNodeIPAddress().equals(this.nodeIPAddress) && (otherNodeInformation.getNodePortNumber() == this.nodePortNumber)) {
				return true;
			}
		}
		return false;
	}
	
	public String getNodeIPAddress() {
		return this.nodeIPAddress;
	}
	
	public int getNodePortNumber() {
		return this.nodePortNumber;
	}
	
	public void addConnection() {
		this.numberOfConnections++;
	}
	
	public int getNumberOfConnections() {
		return this.numberOfConnections;
	}
	
	@Override
	public String toString() {
		return (this.nodeIPAddress + ":" + this.nodePortNumber);
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}
}
