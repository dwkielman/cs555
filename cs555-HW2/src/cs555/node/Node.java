package cs555.node;

import cs555.wireformats.Event;

/**
 * An interface for the Node's onEvent method
 */

public interface Node {
	public void onEvent(Event event);

	public void setLocalHostPortNumber(int localPort);
}
