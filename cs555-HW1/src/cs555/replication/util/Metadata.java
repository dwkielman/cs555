package cs555.replication.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Metadata {

	private int versionInfoNumber;
	private int sequenceNumber; // chunknumber
	private long timestamp;
	private String checksum;
	
	public Metadata(int versionInfoNumber, int sequenceNumber) {
		this.versionInfoNumber = versionInfoNumber;
		this.sequenceNumber = sequenceNumber;
		this.timestamp = System.currentTimeMillis();
	}

	public int getVersionInfoNumber() {
		return versionInfoNumber;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getChecksum() {
		return checksum;
	}
	
	public Metadata(byte[] marshalledBytes) throws IOException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
		DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));
		
		// versionInfoNumber, int
		int versionInfoNumber = din.readInt();
		this.versionInfoNumber = versionInfoNumber;
		
		// sequenceNumber, int
		int sequenceNumber = din.readInt();
		this.sequenceNumber = sequenceNumber;
		
		// timestamp, long
		long timestamp = din.readLong();
		this.timestamp = timestamp;
		
		// checksum, String
		int checksumLength = din.readInt();
		byte[] checksumBytes = new byte[checksumLength];
		din.readFully(checksumBytes);
		this.checksum = new String(checksumBytes);
		
		baInputStream.close();
		din.close();
	}
	
	public byte[] getBytes() throws IOException {
		byte[] marshalledBytes = null;
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));
		
		// versionInfoNumber, int
		dout.writeInt(this.versionInfoNumber);
		
		// sequenceNumber, int
		dout.writeInt(this.sequenceNumber);
		
		// timestamp, long
		dout.writeLong(this.timestamp);
		
		// checksum, String
		byte[] checksumBytes = this.checksum.getBytes();
		int checksumLength = checksumBytes.length;
		dout.writeInt(checksumLength);
		dout.write(checksumBytes);
		
		dout.flush();
		marshalledBytes = baOutputStream.toByteArray();
		baOutputStream.close();
		dout.close();
		
		return marshalledBytes;
	}
	
	// code based on the following site:
	// https://examples.javacodegeeks.com/core-java/security/messagedigest/generate-a-file-checksum-value-in-java/
	// accessed 2019-september-14
	public void generataSHA1Checksum(byte[] chunkData, int sizeOfSlice) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA1");
	        
			int numberOfSlices = (int) Math.ceil(chunkData.length * 1.0 / sizeOfSlice);
	        StringBuffer sb = new StringBuffer("");
	        
	        for (int i = 0; i < numberOfSlices; i++) {
	        	if (i == numberOfSlices - 1) {
	        		messageDigest.update(chunkData, i * sizeOfSlice, chunkData.length % sizeOfSlice);
	        	} else {
	        		messageDigest.update(chunkData, i * sizeOfSlice, sizeOfSlice);
	        	}
	        	
	        	byte[] digestBytes = messageDigest.digest();
	       	 
		        // converts the byte to a hex format
		        for (int j = 0; j < digestBytes.length; j++) {
		            sb.append(Integer.toString((digestBytes[j] & 0xff) + 0x100, 16).substring(1));
		        }
		        sb.append("\n");
	        }
	        
	        this.checksum = sb.toString();
	 
	        System.out.println("Checksum for the File: " + sb.toString());

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public byte[] generateMetadataBytesToWrite(byte[]chunkData) {
		String metadataString = "Version:\n" + this.versionInfoNumber + "\nSequenceNumber:\n" + this.sequenceNumber + "\nTimestamp:\n" + this.timestamp;

		byte[] metadataBytes = metadataString.getBytes();

		return metadataBytes;
	}
	
}
