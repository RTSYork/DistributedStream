package dstream.util;

import dstream.*;
import java.util.*;

public abstract class AbstractDistributedSet<E> extends AbstractSet<E> implements DistributedSet<E>
{
	ComputeGroup grp;

	public AbstractDistributedSet(ComputeGroup grp)
	{
		this.grp = grp;
	}

	@Override
	public ComputeGroup getComputeGroup()
	{
		return grp;
	}
}
