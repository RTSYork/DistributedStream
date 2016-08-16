import dstream.*;
import dstream.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;
import java.util.stream.*;

public class HDFSSort
{
	public static void main(String[] argv)
	{
		if (argv.length < 2)
		{
			System.err.println("Usage: HDFSSort INDIR OUTDIR");
			System.exit(1);
		}
		HDFSStringCollection lines = new HDFSStringCollection(argv[0]);
		HDFSStringCollectionWriter result = new HDFSStringCollectionWriter(argv[1]);
		sort(lines, result);
		result.finish();
	}

	static void sort(DistributedCollection<String> lines, DistributedCollection<String> result)
	{
		// Sort and store results
		lines
			.stream()
			.sorted()
			.localForEach(i -> { result.add(i); });
	}
}
