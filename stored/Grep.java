import java.util.*;
import java.util.regex.*;
import java.util.stream.*;

public class Grep
{
	public static void grep(Collection<String> lines, String pat)
	{
		Pattern p = Pattern.compile(pat);
		lines
			.parallelStream()
			.flatMap(line ->
			{
				ArrayList<String> ms = new ArrayList<String>();
				Matcher m = p.matcher(line);
				while (m.find())
					ms.add(m.group());
				return ms.parallelStream();
			})
			.collect(Collectors.groupingBy(w -> w, HashMap::new, Collectors.counting()))
			.entrySet()
			.stream()
			.sorted((a, b) -> a.getValue() < b.getValue() ? 1 : a.getValue() > b.getValue() ? -1 : 0)
			.forEach(e ->
			{
				System.out.println(e.getValue() + "\t" + e.getKey());
			});
	}
}
