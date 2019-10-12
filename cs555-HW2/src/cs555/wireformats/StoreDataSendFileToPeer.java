package cs555.wireformats;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StoreDataSendFileToPeer implements Event {

	private final int type = Protocol.STOREDATA_SEND_FILE_TO_PEER;
	private String filename;
	private String key;
	private byte[] fileBytes;
	
	public StoreDataSendFileToPeer(String filename, String key, byte[] fileBytes) {
		this.filename = filename;
		this.key = key;
		this.fileBytes = fileBytes;
	}
	
	/**
	 * byte[] construction is as follows:
	 * type
	 * filename
	 * key
	 * fileBytes
	 * @throws IOException 
	 */
	public StoreDataSendFileToPeer(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		int type = din.readInt();
		
		if (type != Protocol.STOREDATA_SEND_FILE_TO_PEER) {
			System.out.println("Invalid Message Type for StoreDataSendFileToPeer");
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

		// fileBytes
		int fileBytesLength = din.readInt();
		byte[] fileBytesBytes = new byte[fileBytesLength];
		din.readFully(fileBytesBytes);
		this.fileBytes = fileBytesBytes;
		
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

		//fileBytes
		int fileBytesLength = this.fileBytes.length;
		dout.writeInt(fileBytesLength);
		dout.write(fileBytes);
		
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

	public byte[] getFileBytes() {
		return fileBytes;
	}

}
