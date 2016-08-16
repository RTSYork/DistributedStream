package dstream;

/**
 * Encapsulates the transport layer.
 * Contains methods for sending and receiving messages on the current compute node.
 */
public abstract class Communicator
{
	/**
	 * Initialises the communicator.
	 * Must be the first method called for a communicator.
	 * @param argv Array of command-line arguments, usually obtained from the main method.
	 */
	abstract public String[] init(String[] argv);

	/**
	 * Finalises the communicator.
	 * Must be the last method called for a communicator.
	 */
	abstract public void cleanup();

	/**
	 * Obtains the current node's rank (ID).
	 * @return Current node's rank.
	 */
	abstract public int getRank();

	/**
	 * Obtains the size of the cluster.
	 * @return Size of cluster.
	 */
	abstract public int getSize();

	/**
	 * Sends a Java object to the specified node with the specified tag.
	 * @arg obj Object to send.
	 * @arg dst Destination node's rank.
	 * @arg tag Tag value.
	 */
	abstract public void sendObject(Object obj, int dst, int tag);

	/**
	 * Receives a Java object with the specified tag.
	 * @arg tag Tag value.
	 * @return The received object.
	 */
	abstract public Object recvObject(int tag);

	/**
	 * Receives a Java object with the specified tag and the node's rank it was sent from.
	 * @arg obj Array of 1 object (for receiving).
	 * @arg tag Tag value.
	 * @return The source node's rank.
	 */
	abstract public int recvObject(Object[] obj, int tag);

	/**
	 * Sends an integer to the referenced node with the specified tag.
	 * @arg n Integer to send.
	 * @arg dst Destination node's rank.
	 * @arg tag Tag value.
	 */
	abstract public void sendInt(int n, int dst, int tag);

	/**
	 * Receives an integer from the specified node with the specified tag.
	 * @arg src Source node's rank.
	 * @arg tag Tag value.
	 * @return The received object.
	 */
	abstract public int recvInt(int src, int tag);
}
