package mem;

import java.io.*;
import java.util.*;

public class UnstructuredCollection extends ArrayList<String>
{
	public UnstructuredCollection(String filename)
	{
		super();
		try
		{
			BufferedReader r = new BufferedReader(new FileReader(filename));
			String line;
			while ((line = r.readLine()) != null)
				add(line);
			r.close();
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
