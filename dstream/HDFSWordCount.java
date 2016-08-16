import dstream.*;
import dstream.util.*;

import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

public class HDFSWordCount
{
	public static void main(String[] argv)
	{
		if (argv.length < 2)
		{
			System.err.println("Usage: HDFSWordCount INDIR OUTDIR");
			System.exit(1);
		}
		HDFSStringCollection lines = new HDFSStringCollection(argv[0]);
		HDFSStringCollectionWriter result = new HDFSStringCollectionWriter(argv[1]);
		wordcount(lines, result);
		result.finish();
	}

	static void wordcount(DistributedCollection<String> lines,
		DistributedCollection<String> result)
	{
		Pattern delim = Pattern.compile("\\s+");
		// Get local word counts
		Map<String, Long> localCount = lines
			.parallelStream()
			.flatMap(line -> Stream.of(delim.split(line)))
			.localCollect(Collectors.groupingByConcurrent(w -> w, Collectors.counting()));
		DistributedMap<String, Long> localCountDistrib = DistributedMap.wrap(localCount);
		// Shuffle
		Map<String, Long> totalCount = localCountDistrib
			.entrySet()
			.parallelStream()
			.map(e -> new AbstractMap.SimpleEntry<>(e)) // Map.Entry is not serialisable
			.distribute(e -> e.getKey().hashCode())
			.localCollect(Collectors.groupingByConcurrent(e -> e.getKey(),
				Collectors.summingLong(e -> (long) e.getValue())));
		DistributedMap<String, Long> totalCountDistrib = DistributedMap.wrap(totalCount);
		// Get total counts
		totalCountDistrib
			.entrySet()
			.stream()
			.localForEach(e -> { result.add("(" + e.getKey() + "," + e.getValue() + ")"); });
	}
}
