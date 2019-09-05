package cs555.replication.wireformats;

import java.io.IOException;

/**
 * Event interface with the getType() and getBytes() defined
 */

public interface Event {

	public int getType();
	
	public byte[] getBytes() throws IOException;
}
