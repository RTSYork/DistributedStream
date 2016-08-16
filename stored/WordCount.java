import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

public class WordCount
{
	public static void wc(Collection<String> lines)
	{
		Pattern delim = Pattern.compile("\\s+");
		lines
			.parallelStream()
			.flatMap(line ->
			{
				Scanner s = new Scanner(line).useDelimiter(delim);
				ArrayList<String> words = new ArrayList<String>();
				while (s.hasNext())
					words.add(s.next());
				s.close();
				return words.parallelStream();
			})
			.collect(Collectors.groupingBy(w -> w, TreeMap::new, Collectors.counting()))
			.entrySet()
			.stream()
			.forEach(e ->
			{
				System.out.println(e.getKey() + "\t" + e.getValue());
			});
	}
}
