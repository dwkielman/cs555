package cs555.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TableEntry {
	
	
	private String identifier;
	private NodeInformation nodeInformation;
	private String nickname;
	
	public TableEntry(String identifier, NodeInformation nodeInformation, String nickname) {
		this.identifier = identifier;
		this.nodeInformation = nodeInformation;
		this.nickname = nickname;
	}
	
	public TableEntry(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

		int identifierLength = din.readInt();
		byte[] identifierBytes = new byte[identifierLength];
		din.readFully(identifierBytes);
		
		// identifier
		this.identifier = new String(identifierBytes);
		
		// NodeInformation
		int nodeInformationLength = din.readInt();
		byte[] nodeInformationBytes = new byte[nodeInformationLength];
		din.readFully(nodeInformationBytes);
		this.nodeInformation = new NodeInformation(nodeInformationBytes);
		
		int nicknameLength = din.readInt();
		byte[] nicknameBytes = new byte[nicknameLength];
		din.readFully(nicknameBytes);
		
		// nickname
		this.nickname = new String(nicknameBytes);
		
		baInputStream.close();
		din.close();
	}
	
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		// identifier
		byte[] identifierBytes = this.identifier.getBytes();
		int identifierLength = identifierBytes.length;
		dout.writeInt(identifierLength);
		dout.write(identifierBytes);
		
		// NodeInformation
		byte[] nodeInformationBytes = this.nodeInformation.getBytes();
		int nodeInformationLength = nodeInformationBytes.length;
		dout.writeInt(nodeInformationLength);
		dout.write(nodeInformationBytes);

		// nickname
		byte[] nicknameBytes = this.nickname.getBytes();
		int nicknameLength = nicknameBytes.length;
		dout.writeInt(nicknameLength);
		dout.write(nicknameBytes);

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof TableEntry) {
			TableEntry otherTableEntry = (TableEntry) o;
			if ((otherTableEntry.getNodeInformation().equals(this.nodeInformation)) && (otherTableEntry.getIdentifier().equals(this.identifier))) {
				return true;
			}
		}
		return false;
	}
	
	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public NodeInformation getNodeInformation() {
		return nodeInformation;
	}

	public void setNodeInformation(NodeInformation nodeInformation) {
		this.nodeInformation = nodeInformation;
	}

	public String getNickname() {
		return nickname;
	}

	public void setNickname(String nickname) {
		this.nickname = nickname;
	}
	
	

}
