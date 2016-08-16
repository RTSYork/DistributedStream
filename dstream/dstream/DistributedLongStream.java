package dstream;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * A distributed extension of Java 8 Streams for long primitive type.
 * The associated pipeline is replicated on each participating compute node.
 * @see java.util.stream.LongStream
 */
public interface DistributedLongStream extends LongStream
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
	 * @return DistributedLongStream consisting of all elements.
	 */
	public DistributedLongStream distribute();

	/**
	 * Sends data elements between nodes in the current compute group
	 * according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param p Programmer-defined partitioner.
	 * @return DistributedLongStream consisting of all elements.
	 */
	public DistributedLongStream distribute(LongPartitioner p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to a hash-based partitioner.
	 * A stateful eager intermediate operation.
	 * Suitable for MapReduce-style shuffling of data.
	 * @param grp Destination compute group.
	 * @return DistributedLongStream consisting of all elements.
	 */
	public DistributedLongStream distribute(ComputeGroup grp);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * another compute group according to the specified partitioner.
	 * A stateful eager intermediate operation.
	 * @param grp Destination compute group.
	 * @param p Programmer-defined partitioner.
	 * @return DistributedLongStream consisting of all elements.
	 */
	public DistributedLongStream distribute(
		ComputeGroup grp, LongPartitioner p);

	/**
	 * Sends data elements from nodes in the current compute group to
	 * the specified node.
	 * A stateful eager intermediate operation.
	 * @param node Destination compute node.
	 * @return DistributedLongStream consisting of all elements.
	 */
	public DistributedLongStream distribute(ComputeNode node);

	/**
	 * Splits the current stream into multiple streams, with each
	 * stream containing all elements of the current stream.
	 * A stateful eager intermediate operation.
	 * @param numStreams Number of streams to create.
	 * @return Array of DistributedLongStreams.
	 */
	public DistributedLongStream[] split(int numStreams);

	/**
	 * Merges multiple streams into a single stream containing all
	 * elements from the source streams.
	 * The resulting stream's compute group is the union of all source
	 * stream compute groups.
	 * A stateful eager intermediate operation.
	 * @param streams Array or argument list of streams to merge.
	 * @return A single DistributedDoubleStream.
	 */
	public DistributedLongStream join(DistributedLongStream... streams);

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
		ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner);

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
	 * @return DistributedLongStream without duplicate local data elements.
	 */
	public DistributedLongStream localDistinct();

	/**
	 * Executes an action on each local element.
	 * Encounter order is not preserved.
	 * Equivalent to executing forEach() on each local stream.
	 * A terminal operation.
	 * @param action Action to apply on each element. Must be non-interfering.
	 */
	public void localForEach(LongConsumer action);

	/**
	 * Executes an action on each local element.
	 * Encounter order is preserved.
	 * Equivalent to executing forEachOrdered() on each local stream.
	 * A terminal operation.
	 * @param action Action to apply on each element. Must be non-interfering.
	 */
	public void localForEachOrdered(LongConsumer action);

	/**
	 * Keeps only the specified number of elements in each local stream.
	 * Equivalent to executing limit() on each local stream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return DistributedLongStream with each local stream
	 * truncated to maxSize elements.
	 */
	public DistributedLongStream localLimit(long maxSize);

	/**
	 * Returns the largest encountered value of a local data element.
	 * Equivalent to executing max() on each local stream.
	 * A terminal operation.
	 * @return The largest local data element, or empty if no elements.
	 */
	public OptionalLong localMax();

	/**
	 * Returns the smallest encountered value of a local data element.
	 * Equivalent to executing min() on each local stream.
	 * A terminal operation.
	 * @return The smallest local data element, or empty if no elements.
	 */
	public OptionalLong localMin();

	/**
	 * Performs an action on each local element and returns the same
	 * DistributedLongStream.
	 * Equivalent to executing peek() on each local stream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same DistributedLongStream.
	 */
	public DistributedLongStream localPeek(LongConsumer action);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param op Associative accumulating function.
	 * @return Either the reduced value or an empty value if no elements.
	 */
	public OptionalLong localReduce(LongBinaryOperator op);

	/**
	 * Accumulates local elements into a single value of the same type.
	 * Equivalent to executing reduce() on each local stream.
	 * A terminal operation.
	 * @param identity The accumulating function's identity value.
	 * @param op Associative accumulating function.
	 * @return The reduced value.
	 */
	public long localReduce(long identity, LongBinaryOperator op);

	/**
	 * Removes the first n elements in each local stream.
	 * Equivalent to executing skip() on each local stream.
	 * An intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return DistributedLongStream with the elements removed.
	 */
	public DistributedLongStream localSkip(long n);

	/**
	 * Sorts all elements in each local stream.
	 * Equivalent to executing sorted() on each local stream.
	 * An intermediate operation.
	 * @return DistributedLongStream with the elements locally sorted.
	 */
	public DistributedLongStream localSorted();

	/**
	 * Returns the sum of all elements in each local stream.
	 * Equivalent to executing sum() on each local stream.
	 * A terminal operation.
	 * @return Sum of local elements.
	 */
	public long localSum();

	/**
	 * Returns statistics (average, sum, etc.) for each local stream.
	 * Equivalent to executing summaryStatistics() on each local stream.
	 * A terminal operation.
	 * @return LongSummaryStatistics containing statistics about each
	 * local stream.
	 */
	public LongSummaryStatistics localSummaryStatistics();

	/**
	 * Collects all local elements into an array.
	 * Equivalent to executing toArray() on each local stream.
	 * A terminal operation.
	 * @return Array containing all local elements.
	 */
	public long[] localToArray();

	// Overrides

	/**
	 * Converts each element into a double.
	 * An intermediate operation.
	 * @return DistributedDoubleStream of converted elements.
	 */
	public DistributedDoubleStream asDoubleStream();

	/**
	 * Converts each element into a Long object.
	 * An intermediate operation.
	 * @return Distributed Stream of converted elements.
	 */
	public DistributedStream<Long> boxed();

	/**
	 * Removes duplicate elements in the DistributedIntStream.
	 * A stateful eager intermediate operation.
	 * @return DistributedLongStream without duplicate data elements.
	 */
	public DistributedLongStream distinct();

	/**
	 * Removes elements in the DistributedLongStream that do not satisfy the
	 * specified predicate.
	 * An intermediate operation.
	 * @param predicate Boolean function that decides whether an element
	 * should remain in the Distributed Stream.
	 * @return DistributedLongStream where all elements satisfy the predicate.
	 */
	public DistributedLongStream filter(LongPredicate predicate);

	/**
	 * Replaces each element of the DistributedLongStream with elements in the
	 * stream returned by the specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning a stream.
	 * @return DistributedLongStream with the applied mapping.
	 */
	public DistributedLongStream flatMap(
		LongFunction<? extends LongStream> mapper);

	/**
	 * Keeps only the specified number of elements in the DistributedLongStream.
	 * An intermediate operation.
	 * @param maxSize Maximum number of elements to keep.
	 * @return DistributedLongStream truncated to maxSize elements.
	 */
	public DistributedLongStream limit(long maxSize);

	/**
	 * Replaces each element of the DistributedLongStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type long.
	 * @return DistributedLongStream with the applied mapping.
	 */
	public DistributedLongStream map(LongUnaryOperator mapper);

	/**
	 * Replaces each element of the DistributedLongStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type double.
	 * @return DistributedDoubleStream with the applied mapping.
	 */
	public DistributedDoubleStream mapToDouble(LongToDoubleFunction mapper);

	/**
	 * Replaces each element of the DistributedLongStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of type long integer.
	 * @return DistributedIntStream with the applied mapping.
	 */
	public DistributedIntStream mapToInt(LongToIntFunction mapper);

	/**
	 * Replaces each element of the DistributedLongStream with values from the
	 * specified mapping function.
	 * An intermediate operation.
	 * @param mapper Mapping function returning values of an arbitrary type.
	 * @return Distributed Stream with the applied mapping.
	 */
	public <U> DistributedStream<U> mapToObj(LongFunction<? extends U> mapper);

	/**
	 * Returns a parallel DistributedLongStream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A parallel DistributedLongStream.
	 */
	public DistributedLongStream parallel();

	/**
	 * Performs an action on each element on all nodes and returns the same
	 * DistributedLongStream.
	 * An intermediate operation.
	 * @param action A non-interfering action.
	 * @return The same DistributedLongStream.
	 */
	public DistributedLongStream peek(LongConsumer action);

	/**
	 * Returns a sequential DistributedLongStream with an otherwise identical
	 * state.
	 * An intermediate operation.
	 * @return A sequential DistributedLongStream.
	 */
	public DistributedLongStream sequential();

	/**
	 * Removes the first n elements in the DistributedLongStream.
	 * A stateful intermediate operation.
	 * @param n Number of local elements to skip.
	 * @return DistributedLongStream with the elements removed.
	 */
	public DistributedLongStream skip(long n);

	/**
	 * Sorts all elements in the DistributedLongStream.
	 * A stateful eager intermediate operation.
	 * @return DistributedLongStream with the elements sorted.
	 */
	public DistributedLongStream sorted();

	// Generators
	public static DistributedLongStream concat(DistributedLongStream a, DistributedLongStream b)
	{
		return null; // TODO
	}

	public static DistributedLongStream empty(ComputeGroup grp)
	{
		return new LongPipeline(LongStream.empty(), grp);
	}

	public static DistributedLongStream empty()
	{
		return empty(ComputeGroup.getCluster());
	}

	public static DistributedLongStream generate(LongSupplier s)
	{
		return null; // TODO
	}

	public static DistributedLongStream iterate(long seed, LongUnaryOperator f)
	{
		return null; // TODO
	}

	public static DistributedLongStream of(long... values)
	{
		return null; // TODO
	}

	public static DistributedLongStream of(long t)
	{
		return null; // TODO
	}

	public static DistributedLongStream range(long startInclusive, long endExclusive, ComputeGroup grp)
	{
		if (grp.contains(ComputeNode.getSelf()))
		{
			long size = grp.size();
			long rank = grp.indexOf(ComputeNode.getSelf());
			return new LongPipeline(LongStream.range(0, (endExclusive - startInclusive + size - rank - 1) / size).map(n -> startInclusive + n * size + rank), grp);
		}
		return DistributedLongStream.empty();
	}

	public static DistributedLongStream range(long startInclusive, long endExclusive)
	{
		return range(startInclusive, endExclusive, ComputeGroup.getCluster());
	}

	public static DistributedLongStream rangeClosed(long startInclusive, long endInclusive, ComputeGroup grp)
	{
		long size = grp.size();
		long rank = ComputeNode.getSelf().comm.getRank();
		return new LongPipeline(LongStream.rangeClosed(startInclusive, (endInclusive + size - 1) / size).map(n -> n * size + rank), grp);
	}

	public static DistributedLongStream rangeClosed(long startInclusive, long endInclusive)
	{
		return rangeClosed(startInclusive, endInclusive, ComputeGroup.getCluster());
	}
}
