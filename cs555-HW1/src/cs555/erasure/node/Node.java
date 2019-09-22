package cs555.erasure.node;

import cs555.erasure.wireformats.Event;

/**
 * An interface for the Node's onEvent method
 */

public interface Node {
	public void onEvent(Event event);

	public void setLocalHostPortNumber(int localPort);
}
