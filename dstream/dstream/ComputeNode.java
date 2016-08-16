package dstream;

import mpi.*;
import dstream.util.*;
import java.lang.reflect.*;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Represents a compute node in the cluster.
 * Compute nodes can be grouped together using compute groups.
 * @see dstream.ComputeGroup
 */
public class ComputeNode
{
	static Communicator comm;
	static ComputeNode thisNode;

	int rank;
	final boolean isMaster;
	private final String hostname;
	private final String name;
	private Map<String,ComputeNode> nodes;

	/**
	 * Fix for common ForkJoinPool not having asyncMode set to true
	 */
	private static void fixCommonPool()
	{
		try
		{
			int parallelism = ForkJoinPool.commonPool().getParallelism();
			ForkJoinPool pool = new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
			Field common = ForkJoinPool.class.getDeclaredField("common");
			common.setAccessible(true);
			Field modifiers = Field.class.getDeclaredField("modifiers");
			modifiers.setAccessible(true);
			modifiers.setInt(common, common.getModifiers() & ~Modifier.FINAL);
			common.set(null, pool);
		}
		catch (NoSuchFieldException | IllegalAccessException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Program entry point on every node.
	 * Communicator initialisation and finalisation is done here.
	 * @param argv Program arguments. argv[0] is the actual class to run which
	 * must have a static main() method.
	 */
	public static void main(String[] argv)
	{
		if (argv.length == 0)
		{
			System.err.println("ComputeNode: No class with main() method specified");
			System.exit(1);
		}
		try
		{
			fixCommonPool();
			comm = new MPJCommunicator();
			argv = comm.init(argv);
			int rank = comm.getRank();
			int size = comm.getSize();
			// Initialise cluster and broadcast hostnames
			ComputeGroup.cluster = new ComputeGroup();
			String[] hostnames = new String[size];
			hostnames[rank] = InetAddress.getLocalHost().getHostName().split("\\.")[0];
			for (int i = 0; i < size; i++)
				if (i != rank)
					comm.sendObject(hostnames[rank], i, 0);
			String[] hostbuf = new String[1];
			for (int i = 1; i < size; i++)
			{
				int src = comm.recvObject(hostbuf, 0);
				hostnames[src] = hostbuf[0];
			}
			for (int i = 0; i < size; i++)
				ComputeGroup.cluster.add(new ComputeNode(i, size, i == rank, hostnames[i]));
			hostnames = null;
			hostbuf = null;
			// Execute main()
			Class c = Class.forName(argv[0]);
			Method m = c.getMethod("main", String[].class);
			String[] newArgv = java.util.Arrays.copyOfRange(argv, 1, argv.length);
			m.invoke(null, (Object) newArgv);
			comm.cleanup();
		}
		catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | UnknownHostException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	private ComputeNode(int rank, int size, boolean self, String hostname)
	{
		this.rank = rank;
		isMaster = (rank == 0);
		this.hostname = hostname;
		name = "node" + Integer.toString(rank);
		nodes = new LinkedHashMap<String,ComputeNode>(size);
		nodes.put(name, this);
		if (self)
			thisNode = this;
	}

	/**
	 * Returns the compute node's name.
	 * @return String containing the node's name.
	 */
	public String getName()
	{
		return name;
	}

	/**
	 * Returns the compute node's communicator.
	 * @return Current node's communicator.
	 */
	public Communicator getCommunicator()
	{
		return comm;
	}

	/**
	 * Tells whether we are executing on this node.
	 * @return True if we are executing on this node, false otherwise.
	 */
	public boolean isSelf()
	{
		return this == thisNode;
	}

	public String getHostname()
	{
		return hostname;
	}

	/**
	 * Returns the node we are executing on.
	 * @return The currently executing node.
	 */
	public static ComputeNode getSelf()
	{
		return thisNode;
	}

	/**
	 * Returns the node whose name matches the specified string.
	 * @param name Name to search for.
	 * @return Node with the matching name.
	 */
	public static ComputeNode findByName(String name)
	{
		return thisNode.nodes.get(name);
	}

	private static AtomicLong tagCount = new AtomicLong(0);

	private static final int MIN_TAG = 1;
	private static final int MAX_TAG = Integer.MAX_VALUE; // BUG: MPI.TAG_UB is zero
	private static final int WRAP_TAG = MAX_TAG - MIN_TAG;

	/**
	 * Returns the next MPI tag for use in group comunications.
	 * Current algorithm uses MPI rank of (group's) root node concatenated with a counter.
	 * @param grp Associated compute group.
	 * @return Tag value.
	 */
	static int nextTag(ComputeGroup grp)
	{
		ComputeNode self = ComputeNode.getSelf();
		if (!grp.contains(self))
			throw new RuntimeException("Node not in group");
		ComputeNode root = grp.get(0);
		int mpiTag = -1;
		if (self == root)
		{
			long tag;
			int clusterSize = ComputeGroup.getCluster().size();
			tag = self.comm.getRank() + clusterSize * tagCount.getAndIncrement();
			mpiTag = (int) (tag % WRAP_TAG) + MIN_TAG;
			for (int i = 1; i < grp.size(); i++)
				comm.sendInt(mpiTag, grp.get(i).rank, 0); // Send tag to other group members
		}
		else
			mpiTag = comm.recvInt(root.comm.getRank(), 0);
		return mpiTag;
	}
}
