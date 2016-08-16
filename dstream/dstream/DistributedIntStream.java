package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A distributed extension of Java 8 Streams for int primitive type.
 * The associated pipeline is replicated on each participating compute node.
 * @see java.util.stream.IntStream
 */
public interface DistributedIntStream extends IntStream
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
	 * @return DistributedIntStream consisting of all elements.
	 */
	public DistributedIntStream distribute();

	/**
	 * Sends data elements between nodes in the current compute group
	 * according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param p Programmer-defined partitioner.
	 * @return DistributedIntStream consisting of all elements.
	 */
	public DistributedIntStream distribute(IntPartitioner p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to a hash-based partitioner.
	 * A stateful eager intermediate operation.
	 * Suitable for MapReduce-style shuffling of data.
	 * @param grp Destination compute group.
	 * @return DistributedIntStream consisting of all elements.
	 */
	public DistributedIntStream distribute(ComputeGroup grp);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param grp Destination compute group.
	 * @param p Programmer-defined partitioner.
	 * @return DistributedIntStream consisting of all elements.
	 */
	public DistributedIntStream distribute(ComputeGroup grp, IntPartitioner p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * the specified node.
	 * A stateful eager intermediate operation.
	 * @param node Destination compute node.
	 * @return DistributedIntStream consisting of all elements.
	 */
	public DistributedIntStream distribute(ComputeNode node);

	/**
	 * Splits the current stream into multiple streams, with each
	 * stream containing all elements of the current stream.
	 * A stateful eager intermediate operation.
	 * @param numStreams Number of streams to create.
	 * @return Array of DistributedIntStreams.
	 */
	public DistributedIntStream[] split(int numStreams);

	/**
	 * Merges multiple streams into a single stream containing all
	 * elements from the source streams.
	 * The resulting stream's compute group is the union of all source
	 * stream compute groups.
	 * A stateful eager intermediate operation.
	 * @param streams Array or argument list of streams to merge.
	 * @return A single DistributedIntStream.
	 */
	public DistributedIntStream join(DistributedIntStream... streams);

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
		ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner);

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
	 * @return DistributedIntStream without duplicate local data elements.
	 */
	public DistributedIntStream localDistinct();

	/**
	 * Keeps only the specified number of elements in each local stream.
	 * Equivalent to executing limit() on each local stream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return DistributedIntStream with each local stream
	 * truncated to maxSize elements.
	 */
	public DistributedIntStream localLimit(long maxSize);

	/**
	 * Returns the largest encountered value of a local data element.
	 * Equivalent to executing max() on each local stream.
	 * A terminal operation.
	 * @return The largest local data element, or empty if no elements.
	 */
	public OptionalInt localMax();

	/**
	 * Returns the smallest encountered value of a local data element.
	 * Equivalent to executing min() on each local stream.
	 * A terminal operation.
	 * @return The smallest local data element, or empty if no elements.
	 */
	public OptionalInt localMin();

	/**
	 * Performs an action on each local element and returns the same
	 * DistributedIntStream.
	 * Equivalent to executing peek() on each local stream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same DistributedIntStream.
	 */
	public DistributedIntStream localPeek(IntConsumer action);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param op Associative accumulating function.
	 * @return Either the reduced value or an empty value if no elements.
	 */
	public OptionalInt localReduce(IntBinaryOperator op);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param identity The accumulating function's identity value.
	 * @param op Associative accumulating function.
	 * @return The reduced value.
	 */
	public int localReduce(int identity, IntBinaryOperator op);

	/**
	 * Removes the first n elements in each local stream.
	 * Equivalent to executing skip() on each local stream.
	 * An intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return DistributedIntStream with the elements removed.
	 */
	public DistributedIntStream localSkip(long n);

	/**
	 * Sorts all elements in each local stream.
	 * Equivalent to executing sorted() on each local stream.
	 * An intermediate operation.
	 * @return DistributedIntStream with the elements locally sorted.
	 */
	public DistributedIntStream localSorted();

	/**
	 * Returns the sum of all elements in each local stream.
	 * Equivalent to executing sum() on each local stream.
	 * A terminal operation.
	 * @return Sum of local elements.
	 */
	public int localSum();

	/**
	 * Returns statistics (average, sum, etc.) for each local stream.
	 * Equivalent to executing summaryStatistics() on each local stream.
	 * A terminal operation.
	 * @return IntSummaryStatistics containing statistics about each
	 * local stream.
	 */
	public IntSummaryStatistics localSummaryStatistics();

	/**
	 * Collects all local elements into an array.
	 * Equivalent to executing toArray() on each local stream.
	 * A terminal operation.
	 * @return Array containing all local elements.
	 */
	public int[] localToArray();

	// Overrides

	/**
	 * Converts each element into a double.
	 * An intermediate operation.
	 * @return DistributedDoubleStream of converted elements.
	 */
	public DistributedDoubleStream asDoubleStream();

	/**
	 * Converts each element into a long integer.
	 * An intermediate operation.
	 * @return DistributedLongStream of converted elements.
	 */
	public DistributedLongStream asLongStream();

	/**
	 * Converts each element into an Integer object.
	 * An intermediate operation.
	 * @return Distributed Stream of converted elements.
	 */
	public DistributedStream<Integer> boxed();

	/**
	 * Removes duplicate elements in the DistributedIntStream.
	 * A stateful eager intermediate operation.
	 * @return DistributedIntStream without duplicate data elements.
	 */
	public DistributedIntStream distinct();

	/**
	 * Removes elements in the DistributedIntStream that do not satisfy the
	 * specified predicate.
	 * An intermediate operation.
	 * @param predicate Boolean function that decides whether an element
	 * should remain in the Distributed Stream.
	 * @return DistributedIntStream where all elements satisfy the predicate.
	 */
	public DistributedIntStream filter(IntPredicate predicate);

	/**
	 * Replaces each element of the DistributedIntStream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream.
	 * @return DistributedIntStream with the applied mapping.
	 */
	public DistributedIntStream flatMap(IntFunction<? extends IntStream> mapper);

	/**
	 * Keeps only the specified number of elements in the DistributedIntStream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return DistributedIntStream truncated to maxSize elements.
	 */
	public DistributedIntStream limit(long maxSize);

	/**
	 * Replaces each element of the DistributedIntStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type integer.
	 * @return DistributedIntStream with the applied mapping.
	 */
	public DistributedIntStream map(IntUnaryOperator mapper);

	/**
	 * Replaces each element of the DistributedIntStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type double.
	 * @return DistributedDoubleStream with the applied mapping.
	 */
	public DistributedDoubleStream mapToDouble(IntToDoubleFunction mapper);

	/**
	 * Replaces each element of the DistributedIntStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type long integer.
	 * @return DistributedLongStream with the applied mapping.
	 */
	public DistributedLongStream mapToLong(IntToLongFunction mapper);

	/**
	 * Replaces each element of the DistributedIntStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of an arbitrary type.
	 * @return Distributed Stream with the applied mapping.
	 */
	public <U> DistributedStream<U> mapToObj(IntFunction<? extends U> mapper);

	/**
	 * Returns a parallel DistributedIntStream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A parallel DistributedIntStream.
	 */
	public DistributedIntStream parallel();

	/**
	 * Performs an action on each element on all nodes and returns the same
	 * DistributedIntStream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same DistributedIntStream.
	 */
	public DistributedIntStream peek(IntConsumer action);

	/**
	 * Returns a sequential DistributedIntStream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A sequential DistributedIntStream.
	 */
	public DistributedIntStream sequential();

	/**
	 * Removes the first n elements in the DistributedIntStream.
	 * A stateful intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return DistributedIntStream with the elements removed.
	 */
	public DistributedIntStream skip(long n);

	/**
	 * Sorts all elements in the DistributedIntStream.
	 * A stateful eager intermediate operation.
	 * @return DistributedIntStream with the elements sorted.
	 */
	public DistributedIntStream sorted();

	// Generators
	public static DistributedIntStream concat(DistributedIntStream a, DistributedIntStream b)
	{
		return null; // TODO
	}

	public static DistributedIntStream empty()
	{
		return null; // TODO
	}

	public static DistributedIntStream generate(IntSupplier s)
	{
		return null; // TODO
	}

	public static DistributedIntStream iterate(int seed, IntUnaryOperator f)
	{
		return null; // TODO
	}

	public static DistributedIntStream of(int... values)
	{
		return null; // TODO
	}

	public static DistributedIntStream of(int t)
	{
		return null; // TODO
	}
}
