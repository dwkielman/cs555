package cs555.replication.node;

import cs555.replication.wireformats.Event;

/**
 * An interface for the Node's onEvent method
 */

public interface Node {
	public void onEvent(Event event);

	public void setLocalHostPortNumber(int localPort);
}
