package dstream;

import java.io.*;

import mpi.*;

/**
 * MPJ Express communicator implementation.
 */
public class MPJCommunicator extends Communicator
{
	public String[] init(String[] argv)
	{
		try
		{
			return MPI.Init(argv);
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return argv;
	}

	public void cleanup()
	{
		try
		{
			MPI.Finalize();
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	public int getRank()
	{
		return MPI.COMM_WORLD.Rank();
	}

	public int getSize()
	{
		return MPI.COMM_WORLD.Size();
	}

	public void sendObject(Object obj, int dst, int tag)
	{
		Object[] buf = new Object[1];
		buf[0] = obj;
		try
		{
			MPI.COMM_WORLD.Send(buf, 0, 1, MPI.OBJECT, dst, tag);
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	public Object recvObject(int tag)
	{
		Object[] buf = new Object[1];
		try
		{
			MPI.COMM_WORLD.Recv(buf, 0, 1, MPI.OBJECT, MPI.ANY_SOURCE, tag);
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return buf[0];
	}

	public int recvObject(Object[] obj, int tag)
	{
		Object[] buf = new Object[1];
		try
		{
			Status status = MPI.COMM_WORLD.Recv(buf, 0, 1, MPI.OBJECT, MPI.ANY_SOURCE, tag);
			obj[0] = buf[0];
			return status.source;
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return -1;
	}

	public void sendInt(int n, int dst, int tag)
	{
		int[] buf = new int[1];
		buf[0] = n;
		try
		{
			MPI.COMM_WORLD.Send(buf, 0, 1, MPI.INT, dst, tag);
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}

	public int recvInt(int src, int tag)
	{
		int[] buf = new int[1];
		try
		{
			MPI.COMM_WORLD.Recv(buf, 0, 1, MPI.INT, src, tag);
		}
		catch (MPIException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		return buf[0];
	}
}
