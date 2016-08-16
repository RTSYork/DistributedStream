package dstream;

/**
 * Function for partitioning data over a predefined compute group.
 */
@FunctionalInterface
public interface Partitioner<T>
{
	/**
	 * Decides which compute node the data element should be sent to.
	 * @param data Data element for consideration.
	 * @return Integer which will be converted into an index of the
	 * compute node in the compute group. Indexes that are out of bounds
	 * will be wrapped around.
	 */
	public int partition(T data);
}
