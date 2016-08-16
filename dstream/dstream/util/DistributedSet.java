package dstream.util;

import java.util.*;

import dstream.*;

public interface DistributedSet<E> extends DistributedCollection<E>, Set<E>
{
	public static <E> DistributedSet<E> wrap(Set<E> s, ComputeGroup grp)
	{
		return new WrappedDistributedSet(s, grp);
	}

	public static <E> DistributedCollection<E> wrap(Set<E> s)
	{
		return new WrappedDistributedSet(s, ComputeGroup.getCluster());
	}
}

class WrappedDistributedSet<E> extends AbstractSet<E> implements DistributedSet<E>
{
	private ComputeGroup grp;
	private Set<E> s;

	public WrappedDistributedSet(Set<E> s, ComputeGroup grp)
	{
		this.s = s;
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
		return s.iterator();
	}

	@Override
	public Spliterator<E> spliterator()
	{
		return s.spliterator();
	}

	@Override
	public int size()
	{
		return s.size();
	}
}
