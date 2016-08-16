package dstream;

import java.util.*;
import java.util.function.*;
import java.util.concurrent.atomic.*;

/**
 * Represents a group of compute nodes.
 * @see dstream.ComputeNode
 */
public class ComputeGroup extends ArrayList<ComputeNode>
{
	static AtomicInteger nextId = new AtomicInteger(0);
	static ComputeGroup cluster = null;
	int id; // MPI tag

	/**
	 * Constructor that creates an empty compute group.
	 */
	public ComputeGroup()
	{
		super();
		id = nextId.getAndIncrement();
	}

	/**
	 * Constructor that creates a compute group from a collection of
	 * compute nodes.
	 * @param nodes Collection of compute nodes. Can also be an existing
	 * compute group since it is a collection of compute nodes.
	 */
	public ComputeGroup(Collection<ComputeNode> nodes)
	{
		this();
		addAll(nodes);
	}

	/**
	 * Constructor that creates a compute group consisting of a single
	 * compute node.
	 * @param node Compute node that will be part of the new group.
	 */
	public ComputeGroup(ComputeNode node)
	{
		this();
		add(node);
	}

	/**
	 * Returns a compute group consisting of all nodes in the cluster.
	 * @return New compute group with all nodes in cluster.
	 */
	public static ComputeGroup getCluster()
	{
		return new ComputeGroup(cluster);
	}

	@Override
	public boolean add(ComputeNode e)
	{
		if (contains(e))
			return false;
		return super.add(e);
	}

	@Override
	public void add(int index, ComputeNode e)
	{
		if (contains(e))
			return;
		super.add(index, e);
	}

	@Override
	public boolean addAll(Collection<? extends ComputeNode> c)
	{
		boolean changed = false;
		for (ComputeNode i: c)
			if (add(i))
				changed = true;
		return changed;
	}

	@Override
	public boolean addAll(int index, Collection<? extends ComputeNode> c)
	{
		boolean changed = false;
		for (ComputeNode i: c)
		{
			if (contains(i))
				continue;
			add(index++, i);
			changed = true;
		}
		return changed;
	}

	@Override
	public void replaceAll(UnaryOperator<ComputeNode> op)
	{
		throw new UnsupportedOperationException();
	}
}
