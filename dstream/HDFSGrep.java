import dstream.*;
import dstream.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.*;

public class HDFSGrep
{
	public static void main(String[] argv)
	{
		if (argv.length < 3)
		{
			System.err.println("Usage: Grep REGEXP INDIR OUTDIR");
			System.exit(1);
		}
		HDFSStringCollection lines = new HDFSStringCollection(argv[1]);
		HDFSStringCollectionWriter result = new HDFSStringCollectionWriter(argv[2]);
		grep(lines, result, argv[0]);
		result.finish();
	}

	static void grep(DistributedCollection<String> lines,
		DistributedCollection<String> result, String re)
	{
		Pattern match = Pattern.compile(re);
		// Find lines containing the regular expression
		DistributedMap<String, Long> localFound = lines
			.parallelStream()
			.flatMap(line ->
			{
				Matcher m = match.matcher(line);
				List<String> li = new ArrayList<>();
				while (m.find())
					li.add(m.group());
				return li.stream();
			})
			// Group local occurrences and count
			.localCollect(Collectors.groupingBy(s -> s, DistributedHashMap<String, Long>::new,
				Collectors.counting()));
		// Merge with all occurrences
		DistributedMap<String, Long> totalFound = localFound
			.entrySet()
			.parallelStream()
			.map(e -> new AbstractMap.SimpleEntry<>(e))
			.distribute(e -> e.getKey().hashCode())
			.localCollect(Collectors.groupingBy(e -> e.getKey(),
				DistributedHashMap<String, Long>::new,
				Collectors.summingLong(e -> (long) e.getValue())));
		localFound = null;
		// Store results
		totalFound
			.entrySet()
			.stream()
			.localForEach(e -> { result.add(e.getValue() + "\t" + e.getKey()); });
	}
}
