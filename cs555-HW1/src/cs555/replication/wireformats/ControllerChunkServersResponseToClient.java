package cs555.replication.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import cs555.replication.util.NodeInformation;


public class ControllerChunkServersResponseToClient implements Event {

	private final int type = Protocol.CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT;
	private int replicationLevel;
	private ArrayList<NodeInformation> chunkServersNodeInfoList;
	
	public ControllerChunkServersResponseToClient(int replicationLevel, ArrayList<NodeInformation> chunkServersNodeInfoList) {
		this.replicationLevel = replicationLevel;
		this.chunkServersNodeInfoList = chunkServersNodeInfoList;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * replicationLevel
	 * chunkServersNodeInfoList
	 * @throws IOException 
	 */
	public ControllerChunkServersResponseToClient(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.CONTROLLER_CHUNKSERVERS_RESPONSE_TO_CLIENT) {
			System.out.println("Invalid Message Type for ControllerChunkServersResponseToClient");
			return;
		}
		
		// replicationLevel
		int replicationLevel = din.readInt();

		this.replicationLevel = replicationLevel;
				

		// chunkServersNodeInfoList
		// declare as size of the replicationLevel that we are being passed
		this.chunkServersNodeInfoList = new ArrayList<>(this.replicationLevel);
		
		for (int i=0; i < this.replicationLevel; i++) {
			int cSNIength = din.readInt();
			byte[] cSNIBytes = new byte[cSNIength];
			din.readFully(cSNIBytes);
			this.chunkServersNodeInfoList.add(new NodeInformation(cSNIBytes));
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
		
		// replicationLevel
		dout.writeInt(replicationLevel);
		
		// chunkServersNodeInfoList
		for (NodeInformation n : this.chunkServersNodeInfoList) {
			byte[] cSNIBytes = n.getBytes();
			int cSNILength = cSNIBytes.length;
			dout.writeInt(cSNILength);
			dout.write(cSNIBytes);
		}

		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	public int getReplicationLevel() {
		return this.replicationLevel;
	}

	public ArrayList<NodeInformation> getChunkServersNodeInfoList() {
		return this.chunkServersNodeInfoList;
	}

}
