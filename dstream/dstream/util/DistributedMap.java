package dstream.util;

import dstream.*;
import java.util.*;

public interface DistributedMap<K, V> extends Map<K, V>
{
	/**
	 * Returns the group of nodes on which the Distributed Collection's
	 * data resides.
	 * This group is used as the initial compute group for Distributed
	 * Streams backed by the Distributed Collection
	 * @return Distributed Collection's compute group.
	 */
	public ComputeGroup getComputeGroup();

	public DistributedCollection<V> values();

	public DistributedSet<K> keySet();
	public DistributedSet<Map.Entry<K, V>> entrySet();

	public static <K, V> DistributedMap<K, V> wrap(Map<K, V> m, ComputeGroup grp)
	{
		return new WrappedDistributedMap(m, grp);
	}

	public static <K, V> DistributedMap<K, V> wrap(Map<K, V> m)
	{
		return new WrappedDistributedMap(m, ComputeGroup.getCluster());
	}
}

class WrappedDistributedMap<K, V> extends AbstractMap<K, V> implements DistributedMap<K, V>
{
	private ComputeGroup grp;
	private Map<K, V> m;

	public WrappedDistributedMap(Map<K, V> m, ComputeGroup grp)
	{
		this.m = m;
		this.grp = grp;
	}

	@Override
	public ComputeGroup getComputeGroup()
	{
		return grp;
	}

	@Override
	public DistributedSet<Map.Entry<K, V>> entrySet()
	{
		return DistributedSet.wrap(m.entrySet(), grp);
	}

	@Override
	public DistributedSet<K> keySet()
	{
		return DistributedSet.wrap(m.keySet(), grp);
	}

	@Override
	public DistributedCollection<V> values()
	{
		return DistributedCollection.wrap(m.values(), grp);
	}

	@Override
	public int size()
	{
		return m.size();
	}
}
