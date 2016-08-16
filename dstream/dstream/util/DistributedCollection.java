package dstream.util;

import dstream.*;
import java.util.*;

/**
 * Represents a set of collections on participating compute nodes containing
 * data that is part of a distributed dataset.
 */
public interface DistributedCollection<E> extends Collection<E>
{
	/**
	 * Returns the group of nodes on which the Distributed Collection's
	 * data resides.
	 * This group is used as the initial compute group for Distributed
	 * Streams backed by the Distributed Collection
	 * @return Distributed Collection's compute group.
	 */
	public ComputeGroup getComputeGroup();

	/**
	 * Returns a sequential Distributed Stream backed by this
	 * Distributed Collection.
	 * @return A new Distributed Stream.
	 */
	@Override
	public default DistributedStream<E> stream()
	{
		DistributedStream<E> s = DistributedStreamSupport.stream(
			spliterator(), false);
		s.setComputeGroup(getComputeGroup());
		return s;
	}

	/**
	 * Returns a parallel Distributed Stream backed by this
	 * Distributed Collection.
	 * @return A new Distributed Stream.
	 */
	@Override
	public default DistributedStream<E> parallelStream()
	{
		DistributedStream<E> s = DistributedStreamSupport.stream(
			spliterator(), true);
		s.setComputeGroup(getComputeGroup());
		return s;
	}

	public static <E> DistributedCollection<E> wrap(Collection<E> c, ComputeGroup grp)
	{
		return new WrappedDistributedCollection(c, grp);
	}

	public static <E> DistributedCollection<E> wrap(Collection<E> c)
	{
		return new WrappedDistributedCollection(c, ComputeGroup.getCluster());
	}
}

class WrappedDistributedCollection<E> extends AbstractCollection<E> implements DistributedCollection<E>
{
	private ComputeGroup grp;
	private Collection<E> c;

	public WrappedDistributedCollection(Collection<E> c, ComputeGroup grp)
	{
		this.c = c;
		this.grp = grp;
	}

	@Override
	public ComputeGroup getComputeGroup()
	{
		return grp;
	}

	@Override
	public Iterator<E> iterator()
	{
		return c.iterator();
	}

	@Override
	public Spliterator<E> spliterator()
	{
		return c.spliterator();
	}

	@Override
	public int size()
	{
		return c.size();
	}
}
