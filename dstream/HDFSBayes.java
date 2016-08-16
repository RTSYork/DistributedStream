import dstream.*;
import dstream.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// Naive Bayes trainer and classifier
public class HDFSBayes
{
	private static void usage()
	{
		System.err.println("Usage:");
		System.err.println("    HDFSBayes train INFILE TRAINFILE");
		System.err.println("    HDFSBayes classify INFILE OUTFILE TRAINFILE");
		System.exit(1);
	}

	public static void main(String[] argv)
	{
		if (argv.length < 3)
			usage();
		HDFSStringCollection lines = new HDFSStringCollection(argv[1]);
		if (argv[0].equals("train"))
			train(lines, argv[2]);
		else if (argv[0].equals("classify"))
		{
			if (argv.length < 4)
				usage();
			HDFSStringCollectionWriter out = new HDFSStringCollectionWriter(argv[2]);
			classify(lines, out, argv[3]);
			out.finish();
		}
		else
			usage();
	}

	static void train(DistributedCollection<String> lines, String outFile)
	{
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/conf/core-site.xml"));
		try
		{
			fs = FileSystem.get(conf);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		ComputeNode root = ComputeGroup.getCluster().get(0);
		ComputeGroup rootGroup = new ComputeGroup(root);
		Pattern delim = Pattern.compile("\\s+");
		Map<String, P<HashMap<String, Integer>, Integer>> models_rk = lines
			.parallelStream()
			// Train every line
			.map(line -> {
				HashMap<String, Integer> wcount = new HashMap<>();
				String[] tokens = delim.split(line);
				String category = tokens[0];
				for (int i = 0; i < tokens.length; i++)
					wcount.put(tokens[i], 1);
				return new P<>(category, new P<>(wcount, 1));
			})
			// Merge all classifiers
			.distribute(rootGroup)
			.localCollect(Collectors.groupingBy(p -> p.k,
				Collector.of(() -> new P<>(new HashMap<>(), 0), (p, a) -> {
				Map<String, Integer> wcount = p.k;
				for (String word: a.v.k.keySet())
					wcount.put(word, wcount.getOrDefault(word, 0) + 1);
				p.v += a.v.v;
			}, (p1, p2) ->
			{
				Map<String, Integer> wcount = p1.k;
				for (String word: p2.k.keySet())
					wcount.put(word, wcount.getOrDefault(word, 0) + 1);
				p1.v += p2.v;
				return p1;
			})));
		// Calculate probabilities
		if (ComputeNode.getSelf() == root)
		{
			Map<String, HashMap<String, Double>> models_p = models_rk
				.entrySet()
				.parallelStream()
				.map(p -> {
					HashMap<String, Double> prob = new HashMap<>();
					Map<String, Integer> wcount = p.getValue().getKey();
					double ccount = (double) p.getValue().getValue();
					for (String word: wcount.keySet())
						prob.put(word, (double) wcount.getOrDefault(word, 0) / ccount);
					return new P<>(p.getKey(), prob);
				})
				.collect(Collectors.toMap(P::getKey, P::getValue));

			try
			{
				ObjectOutputStream out = new ObjectOutputStream(fs.create(new Path(outFile), false));
				out.writeObject(models_p);
				out.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	static void classify(DistributedCollection<String> lines,
		DistributedCollection<String> out, String trainFile)
	{
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/conf/core-site.xml"));
		try
		{
			fs = FileSystem.get(conf);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		Pattern delim = Pattern.compile("\\s+");
		// Read models into memory
		Map<String, Map<String, Double>> models;
		try
		{
			ObjectInputStream in = new ObjectInputStream(fs.open(new Path(trainFile)));
			models = (Map<String, Map<String, Double>>) in.readObject();
			in.close();
		}
		catch (IOException | ClassNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
			return;
		}
		lines
			.parallelStream()
			.map(line -> {
				// Get words in line with no duplicates
				Set<String> words = Arrays.stream(delim.split(line)).collect(Collectors.toSet());
				double maxp = -1.0;
				String result = "default";
				// Iterate through each model and compute most probable model
				for (Map.Entry<String, Map<String, Double>> m: models.entrySet())
				{
					double p = 1.0;
					Map<String, Double> model = m.getValue();
					for (String word: words)
						p *= model.getOrDefault(word, 0.0001);
					if (p > maxp)
					{
						maxp = p;
						result = m.getKey();
					}
				}
				return result + " " + line;
			})
			.localForEach(line -> {
				out.add(line);
			});
	}

	// Serialisable key-value pair
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
