import dstream.*;
import dstream.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.*;

public class HDFSConnComponents
{
	public static void main(String[] argv)
	{
		if (argv.length < 2)
		{
			System.err.println("Usage: HDFSConnComponents INFILE OUTDIR");
			System.exit(1);
		}
		HDFSStringCollection lines = new HDFSStringCollection(argv[0]);
		HDFSStringCollectionWriter outVertices =
			new HDFSStringCollectionWriter(argv[1] + "/vertices");
		HDFSStringCollectionWriter outSummary =
			new HDFSStringCollectionWriter(argv[1] + "/summary");
		cc(lines, outVertices, outSummary, 0);
	}

	static Queue<HashSet<Long>> localMerge(Queue<HashSet<Long>> q)
	{
		boolean changed;
		do
		{
			Queue<HashSet<Long>> q2 = new ConcurrentLinkedQueue<>();
			changed = false;
			while (!q.isEmpty())
			{
				HashSet<Long> s1 = q.remove();
				if (s1.isEmpty())
					continue;
				for (HashSet<Long> s2: q)
				{
					if (!Collections.disjoint(s1, s2))
					{
						s1.addAll(s2);
						s2.clear();
						changed = true;
					}
				}
				q2.add(s1);
			}
			q = q2;
		}
		while (changed);
		return q;
	}

	static void cc(DistributedCollection<String> lines, DistributedCollection<String> result,
		DistributedCollection<String> summary, int iterations)
	{
		Pattern delim = Pattern.compile("\\t");
		// Load edges
		Queue<HashSet<Long>> localComponents = new ConcurrentLinkedQueue<>();
		lines
			.parallelStream()
			.flatMap(line -> {
				String[] ints = delim.split(line);
				if (ints.length != 2)
				{
					System.err.println("Invalid line format");
					System.exit(1);
				}
				long v1 = Long.parseLong(ints[0]);
				long v2 = Long.parseLong(ints[1]);
				if (v1 < v2)
					return Stream.of(new Edge(v1, v2));
				if (v1 > v2)
					return Stream.of(new Edge(v2, v1));
				return null;
			})
			.distribute(e -> (int) e.v1)
			.localForEach(e ->
			{
				HashSet<Long> s = new HashSet<>();
				s.add(e.v1);
				s.add(e.v2);
				localComponents.add(s);
			});
		// Merge local components
		Queue<HashSet<Long>> localMergedComponents = localMerge(localComponents);
		DistributedCollection<HashSet<Long>> components =
			DistributedCollection.wrap(localMergedComponents);
		// Merge all components: send to first node
		ComputeNode node0 = ComputeGroup.getCluster().get(0);
		boolean isNode0 = node0.isSelf();
		components
			.parallelStream()
			.filter(s -> !isNode0) // Don't send components in node0
			.distribute(node0)
			.localForEach(s -> { localMergedComponents.add(s); });
		if (!isNode0)
			localMergedComponents.clear();
		// Merge local components again
		DistributedCollection<HashSet<Long>> allComponents =
			DistributedCollection.wrap(localMerge(localMergedComponents));
		// Output number of components
		long count = allComponents
			.parallelStream()
			.count();
		System.out.println(ComputeNode.getSelf().getName() + ": There are " +
			count + " components");
		// Output vertices and their component IDs
		// Output summary (number of vertices in each component; ie. number of
		//   vertices with same component ID)
		allComponents
			.stream()
			.localForEach(s ->
			{
				List<Long> sorted = new ArrayList<>(s);
				Collections.sort(sorted);
				long root = sorted.get(0);
				for (Long i: sorted)
					result.add(i + "\t" + root);
				summary.add(root + "\t" + s.size());
			});
	}
}

// Represents a directed edge from v1 to v2
class Edge implements Serializable
{
	public long v1, v2; // Vertices: v1 -> v2

	public Edge(long v1, long v2)
	{
		this.v1 = v1;
		this.v2 = v2;
	}
}
