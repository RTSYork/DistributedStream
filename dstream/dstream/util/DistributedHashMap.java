package dstream.util;

import dstream.*;
import java.util.*;

public class DistributedHashMap<K, V> extends HashMap<K, V> implements DistributedMap<K, V>
{
	private ComputeGroup grp;

	public DistributedHashMap(ComputeGroup grp)
	{
		this.grp = grp;
	}

	public DistributedHashMap()
	{
		this(ComputeGroup.getCluster());
	}

	@Override
	public ComputeGroup getComputeGroup()
	{
		return grp;
	}

	@Override
	public DistributedCollection<V> values()
	{
		return DistributedCollection.wrap(super.values(), grp);
	}

	@Override
	public DistributedSet<Map.Entry<K, V>> entrySet()
	{
		Set<Map.Entry<K, V>> s = super.entrySet();
		return new AbstractDistributedSet<Map.Entry<K, V>>(grp)
		{
			@Override
			public int size()
			{
				return s.size();
			}

			@Override
			public Iterator<Map.Entry<K, V>> iterator()
			{
				return s.iterator();
			}

			@Override
			public Spliterator<Map.Entry<K, V>> spliterator()
			{
				return s.spliterator();
			}
		};
	}

	@Override
	public DistributedSet<K> keySet()
	{
		Set<K> s = super.keySet();
		return new AbstractDistributedSet<K>(grp)
		{
			@Override
			public int size()
			{
				return s.size();
			}

			@Override
			public Iterator<K> iterator()
			{
				return s.iterator();
			}

			@Override
			public Spliterator<K> spliterator()
			{
				return s.spliterator();
			}
		};
	}
}
