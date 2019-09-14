package cs555.replication.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class FileSplitter {
	
	private File file;
	private ArrayList<byte[]> filesAsBytesList;
	private int chunkSize;
	
	public FileSplitter() {};
	
	public FileSplitter(File file, int chunkSize) {
		this.file = file;
		this.chunkSize = chunkSize;
		filesAsBytesList = new ArrayList<byte[]>();
	}

	public ArrayList<byte[]> splitFileIntoBytes() {
		byte[] chunkSizeBytes = new byte[this.chunkSize];
		
		
		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(this.file));
			int fileLength = 0;
			
			while ((fileLength = bis.read(chunkSizeBytes)) > -1) {
				chunkSizeBytes = Arrays.copyOf(chunkSizeBytes, fileLength);
				filesAsBytesList.add(chunkSizeBytes);
				chunkSizeBytes = new byte[this.chunkSize];
			}
		
		} catch  (IOException ioe) {
			ioe.printStackTrace();
		}
		
		return filesAsBytesList;
	}
	
}
