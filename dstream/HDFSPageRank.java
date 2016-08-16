import dstream.*;
import dstream.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

public class HDFSPageRank
{
	public static void main(String[] argv)
	{
		if (argv.length != 3)
		{
			System.err.println("Usage: HDFSPageRank INFILE OUTDIR ITERATIONS");
			System.exit(1);
		}
		HDFSStringCollection lines = new HDFSStringCollection(argv[0]);
		HDFSStringCollectionWriter out = new HDFSStringCollectionWriter(argv[1]);
		int iter = Integer.parseInt(argv[2]);
		pagerank(lines, out, iter);
		out.finish();
	}

	static void pagerank(DistributedCollection<String> lines,
		DistributedCollection<String> out, int iterations)
	{
		Pattern delim = Pattern.compile("\\t");
		// Directed edges: (from, (to1, to2, to3))
		System.out.println("Loading links");
		Map<String, List<String>> localLinks = lines
			.parallelStream()
			.map(s -> {
				String[] parts = delim.split(s);
				return new P<String, String>(parts[0], parts[1]);
			})
			.distribute(p -> p.getKey().hashCode())
			.localDistinct()
			.localCollect(Collectors.groupingByConcurrent(p -> p.getKey(),
				Collectors.mapping(p -> p.getValue(), Collectors.toList())));
		DistributedMap<String, List<String>> links = DistributedMap.wrap(localLinks);

		System.out.println(links.size() + " links loaded.");
		// Initialise ranks to 1.0
		Map<String, Double> localRanks = links
			.keySet()
			.parallelStream()
			.localCollect(Collectors.toConcurrentMap(s -> s, s -> 1.0,
				(a, b) -> a));
		DistributedMap<String, Double> ranks = DistributedMap.wrap(localRanks);

		for (int it = 0; it < iterations; it++)
		{
			System.out.println("Iteration " + (it + 1));
			final DistributedMap<String, Double> curRanks = ranks;
			// Calculate contribution to destinations
			Map<String, Double> localContribs = links
				.entrySet()
				.parallelStream()
				.flatMap(e -> {
					double size = (double) e.getValue().size();
					return e
						.getValue()
						.stream()
						.map(url -> new P<String, Double>(url,
							curRanks.getOrDefault(e.getKey(), 0.0) / size));
				})
				.distribute(p -> p.getKey().hashCode())
				// Add contributions for each destination together
				.localCollect(Collectors.toConcurrentMap(
					p -> p.getKey(), p -> p.getValue(),
					(v1, v2) -> v1 + v2));
			DistributedMap<String, Double> contribs = DistributedMap.wrap(localContribs);
			// Adjust ranks
			localRanks = contribs
				.entrySet()
				.parallelStream()
				.map(p -> new P<>(p.getKey(), p.getValue() * 0.85 + 0.15))
				.localCollect(Collectors.toConcurrentMap(P::getKey, P::getValue, (v1, v2) -> v1));
			ranks = DistributedMap.wrap(localRanks);
		}
	}

	// Represents a key-value pair
	private static class P<T extends Serializable, U extends Serializable>
		implements Serializable
	{
		public T k; // key
		public U v; // value

		public P(T k, U v)
		{
			this.k = k;
			this.v = v;
		}

		public T getKey()
		{
			return k;
		}

		public U getValue()
		{
			return v;
		}
	}
}
