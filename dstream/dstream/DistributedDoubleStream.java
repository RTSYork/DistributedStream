package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A distributed extension of Java 8 Streams for double primitive type.
 * The associated pipeline is replicated on each participating compute node.
 * @see java.util.stream.DoubleStream
 */
public interface DistributedDoubleStream extends DoubleStream
{
	/**
	 * Returns the current Distributed Stream's compute group.
	 * This group determines the participating nodes in the computation.
	 * @return Current Distributed Stream's compute group.
	 */
	public ComputeGroup getComputeGroup();

	/**
	 * Changes the current Distributed Stream's compute group.
	 * @param grp New compute group.
	 */
	public void setComputeGroup(ComputeGroup grp);

	// Data distribution operations
	/**
	 * Sends data elements between nodes in the current compute group
	 * according to a hash-based partitioner.
	 * A stateful eager intermediate operation.
	 * Hash function is the Object.hashCode method.
	 * Elements with the same hash value are sent to the same destination.
	 * Suitable for MapReduce-style shuffling of data.
	 * @return DistributedDoubleStream consisting of all elements.
	 */
	public DistributedDoubleStream distribute();

	/**
	 * Sends data elements between nodes in the current compute group
	 * according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param p Programmer-defined partitioner.
	 * @return DistributedDoubleStream consisting of all elements.
	 */
	public DistributedDoubleStream distribute(DoublePartitioner p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to a hash-based partitioner.
	 * A stateful eager intermediate operation.
	 * Suitable for MapReduce-style shuffling of data.
	 * @param grp Destination compute group.
	 * @return DistributedDoubleStream consisting of all elements.
	 */
	public DistributedDoubleStream distribute(ComputeGroup grp);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param grp Destination compute group.
	 * @param p Programmer-defined partitioner.
	 * @return DistributedDoubleStream consisting of all elements.
	 */
	public DistributedDoubleStream distribute(
		ComputeGroup grp, DoublePartitioner p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * the specified node.
	 * A stateful eager intermediate operation.
	 * @param node Destination compute node.
	 * @return DistributedDoubleStream consisting of all elements.
	 */
	public DistributedDoubleStream distribute(ComputeNode node);

	/**
	 * Splits the current stream into multiple streams, with each
	 * stream containing all elements of the current stream.
	 * A stateful eager intermediate operation.
	 * @param numStreams Number of streams to create.
	 * @return Array of DistributedDoubleStreams.
	 */
	public DistributedDoubleStream[] split(int numStreams);

	/**
	 * Merges multiple streams into a single stream containing all
	 * elements from the source streams.
	 * The resulting stream's compute group is the union of all source
	 * stream compute groups.
	 * A stateful eager intermediate operation.
	 * @param streams Array or argument list of streams to merge.
	 * @return A single DistributedDoubleStream.
	 */
	public DistributedDoubleStream join(DistributedDoubleStream... streams);

	// Local operations

	/**
	 * Returns the mean of all local data elements.
	 * Equivalent to executing average() on each local stream.
	 * A terminal operation.
	 * @return The mean of local data elements, or empty if no elements.
	 */
	public OptionalDouble localAverage();

	/**
	 * Accumulates local data elements on each node into a container.
	 * Equivalent to executing collect() on each local stream.
	 * A terminal operation.
	 * @param supplier Function returning a new container.
	 * @param accumulator Function to add a new element into a container.
	 * @param combiner Function for combining two containers.
	 * @return The resulting container.
	 */
	public <R> R localCollect(Supplier<R> supplier,
		ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner);

	/**
	 * Returns the number of data elements in the local stream.
	 * Equivalent to executing count() on each local stream.
	 * A terminal operation.
	 * @return The number of local data elements.
	 */
	public long localCount();

	/**
	 * Removes duplicate elements in the local stream.
	 * Equivalent to executing distinct() on each local stream.
	 * An intermediate operation.
	 * @return DistributedDoubleStream without duplicate local data elements.
	 */
	public DistributedDoubleStream localDistinct();

	/**
	 * Keeps only the specified number of elements in each local stream.
	 * Equivalent to executing limit() on each local stream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return DistributedDoubleStream with each local stream
	 * truncated to maxSize elements.
	 */
	public DistributedDoubleStream localLimit(long maxSize);

	/**
	 * Returns the largest encountered value of a local data element.
	 * Equivalent to executing max() on each local stream.
	 * A terminal operation.
	 * @return The largest local data element, or empty if no elements.
	 */
	public OptionalDouble localMax();

	/**
	 * Returns the smallest encountered value of a local data element.
	 * Equivalent to executing min() on each local stream.
	 * A terminal operation.
	 * @return The smallest local data element, or empty if no elements.
	 */
	public OptionalDouble localMin();

	/**
	 * Performs an action on each local element and returns the same
	 * DistributedDoubleStream.
	 * Equivalent to executing peek() on each local stream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same DistributedDoubleStream.
	 */
	public DistributedDoubleStream localPeek(DoubleConsumer action);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param op Associative accumulating function.
	 * @return Either the reduced value or an empty value if no elements.
	 */
	public OptionalDouble localReduce(DoubleBinaryOperator op);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param identity The accumulating function's identity value.
	 * @param op Associative accumulating function.
	 * @return The reduced value.
	 */
	public double localReduce(double identity, DoubleBinaryOperator op);

	/**
	 * Removes the first n elements in each local stream.
	 * Equivalent to executing skip() on each local stream.
	 * An intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return DistributedDoubleStream with the elements removed.
	 */
	public DistributedDoubleStream localSkip(long n);

	/**
	 * Sorts all elements in each local stream.
	 * Equivalent to executing sorted() on each local stream.
	 * An intermediate operation.
	 * @return DistributedDoubleStream with the elements locally sorted.
	 */
	public DistributedDoubleStream localSorted();

	/**
	 * Returns the sum of all elements in each local stream.
	 * Equivalent to executing sum() on each local stream.
	 * A terminal operation.
	 * @return Sum of local elements.
	 */
	public double localSum();

	/**
	 * Returns statistics (average, sum, etc.) for each local stream.
	 * Equivalent to executing summaryStatistics() on each local stream.
	 * A terminal operation.
	 * @return DoubleSummaryStatistics containing statistics about each
	 * local stream.
	 */
	public DoubleSummaryStatistics localSummaryStatistics();

	/**
	 * Collects all local elements into an array.
	 * Equivalent to executing toArray() on each local stream.
	 * A terminal operation.
	 * @return Array containing all local elements.
	 */
	public double[] localToArray();

	// Overrides

	/**
	 * Converts each element into a Double object.
	 * An intermediate operation.
	 * @return Distributed Stream of converted elements.
	 */
	public DistributedStream<Double> boxed();

	/**
	 * Removes duplicate elements in the DistributedDoubleStream.
	 * A stateful eager intermediate operation.
	 * @return DistributedDoubleStream without duplicate data elements.
	 */
	public DistributedDoubleStream distinct();

	/**
	 * Removes elements in the DistributedDoubleStream that do not satisfy the
	 * specified predicate.
	 * An intermediate operation.
	 * @param predicate Boolean function that decides whether an element
	 * should remain in the Distributed Stream.
	 * @return DistributedDoubleStream where all elements satisfy the predicate.
	 */
	public DistributedDoubleStream filter(DoublePredicate predicate);

	/**
	 * Replaces each element of the DistributedDoubleStream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream.
	 * @return DistributedDoubleStream with the applied mapping.
	 */
	public DistributedDoubleStream flatMap(
		DoubleFunction<? extends DoubleStream> mapper);

	/**
	 * Keeps only the specified number of elements in the DistributedDoubleStream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return DistributedDoubleStream truncated to maxSize elements.
	 */
	public DistributedDoubleStream limit(long maxSize);

	/**
	 * Replaces each element of the DistributedDoubleStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type integer.
	 * @return DistributedDoubleStream with the applied mapping.
	 */
	public DistributedDoubleStream map(DoubleUnaryOperator mapper);

	/**
	 * Replaces each element of the DistributedDoubleStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type long integer.
	 * @return DistributedIntStream with the applied mapping.
	 */
	public DistributedIntStream mapToInt(DoubleToIntFunction mapper);

	/**
	 * Replaces each element of the DistributedDoubleStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type long integer.
	 * @return DistributedLongStream with the applied mapping.
	 */
	public DistributedLongStream mapToLong(DoubleToLongFunction mapper);

	/**
	 * Replaces each element of the DistributedDoubleStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of an arbitrary type.
	 * @return Distributed Stream with the applied mapping.
	 */
	public <U> DistributedStream<U> mapToObj(DoubleFunction<? extends U> mapper);

	/**
	 * Returns a parallel DistributedDoubleStream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A parallel DistributedDoubleStream.
	 */
	public DistributedDoubleStream parallel();

	/**
	 * Performs an action on each element on all nodes and returns the same
	 * DistributedDoubleStream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same DistributedDoubleStream.
	 */
	public DistributedDoubleStream peek(DoubleConsumer action);

	/**
	 * Returns a sequential DistributedDoubleStream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A sequential DistributedDoubleStream.
	 */
	public DistributedDoubleStream sequential();

	/**
	 * Removes the first n elements in the DistributedDoubleStream.
	 * A stateful intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return DistributedDoubleStream with the elements removed.
	 */
	public DistributedDoubleStream skip(long n);

	/**
	 * Sorts all elements in the DistributedDoubleStream.
	 * A stateful eager intermediate operation.
	 * @return DistributedDoubleStream with the elements sorted.
	 */
	public DistributedDoubleStream sorted();

	// Generators
	public static DistributedDoubleStream concat(DoubleStream a, DoubleStream b)
	{
		return null; // TODO
	}

	public static DoubleStream empty()
	{
		return null; // TODO
	}

	public static DistributedDoubleStream generate(DoubleSupplier s)
	{
		return null; // TODO
	}

	public static DistributedDoubleStream iterate(double seed, DoubleUnaryOperator f)
	{
		return null; // TODO
	}

	public static DistributedDoubleStream of(double... values)
	{
		return null; // TODO
	}

	public static DistributedDoubleStream of(double t)
	{
		return null; // TODO
	}
}
