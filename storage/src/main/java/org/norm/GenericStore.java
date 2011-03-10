package org.norm;


/**
 * A simple interface for network reachable data stores that can be reached with a host and a port.
 */
public interface GenericStore extends Store {
	void setHost(String host);
	void setPort(int port);
}
