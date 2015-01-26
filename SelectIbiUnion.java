import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.*;

/**
 * SelectIbiUnion is a multithreaded JAVA program that executes a SELECT
 * statement into the IBI_UNION database table. Two types of selects are
 * performed: 1) WHERE SHAN_SHA = # and 2) WHERE SCAN_ID = #. It determines the
 * number of selects per second by query using MemSQL.
 * 
 * @author andrewfisher
 */
public class SelectIbiUnion {
	/*
	 * NUM_WORKERS defines the total number of threads that will be used in this
	 * benchmarking test of MemSQL.
	 */
	public static final int NUM_WORKERS = 50;

	/*
	 * NUM_QUERIES_PER_WORKER is how many selects each thread should complete
	 * before ending.
	 */
	public static final int NUM_QUERIES_PER_WORKER = 10000;

	/*
	 * Since the SCAN_SHA column is VARCHAR(11), the first three digits of the
	 * SCAN_SHA for any added record will be the three digit thread ID followed
	 * by an eight digit zero padded number that cannot exceed 9,999,999.
	 */
	public static final int MAX_WORKER_SCAN_SHA = 99999999;
	
	/*
	 * WORKER_TYPE_SCAN_SHA and WORKER_TYPE_SCAN_ID are constants used to
	 * differentiate the conditional used in the WHERE clause of each select
	 * statement.
	 */
	public static final String WORKER_TYPE_SCAN_SHA = "SCAN_SHA";
	public static final String WORKER_TYPE_SCAN_ID = "SCAN_ID";

	/*
	 * main method instantiates WorkerSelect threads and starts them. A
	 * WorkerSelect thread performs benchmarking of MemSQL. At the very
	 * end it prints the total number of selects per second.
	 */
	public static void main(String[] args) throws InterruptedException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		}
		
		final SelectQueriesPerSecond mQps = new SelectQueriesPerSecond();
		
		// create worker threads
		ArrayList<WorkerSelect> workers = new ArrayList<WorkerSelect>();
		for (int i = 0; i < NUM_WORKERS; i++) {
			workers.add(new WorkerSelect(mQps, i));
		}
		
        System.out.println("  Thread:       Queries / Second:       Worker Type:      # Rows Selected:      Total Query Time (ns):");

		// start all the workers
		Iterator<WorkerSelect> i1 = workers.iterator();
		while (i1.hasNext()) {
			WorkerSelect wi = (WorkerSelect) i1.next();
			wi.start();
		}
		
		// wait for all the workers to finish
		Iterator<WorkerSelect> i2 = workers.iterator();
		while (i2.hasNext()) {
			WorkerSelect wi = (WorkerSelect) i2.next();
			wi.join();
		}
		
        System.out.println("--------------------------------------------");
        System.out.println("   TOTAL:       " + mQps.getScanIdQps() + "      " + WORKER_TYPE_SCAN_ID);
        System.out.println("   TOTAL:       " + mQps.getScanShaQps() + "      " + WORKER_TYPE_SCAN_SHA);
	}
}

/**
 * WorkerSelect is a thread that connects to MemSQL, defines a prepared SELECT
 * SQL statement, then executes the query for NUM_QUERIES_PER_WORKER times.
 * Based on the worker thread type, the WHERE conditional in the SELECT query
 * will use either SCAN_ID or SCAN_SHA_ID. At the end it prints the number of
 * selects per second for the thread along with the worker type. The selects
 * per second are also added to a global counter.
 * 
 * @author andrewfisher
 */
class WorkerSelect extends Thread {
	private String workerType;
	private SelectQueriesPerSecond mQps;
	private String mThreadId;
	private long mQueryStartTime;
	private long mQueryTime;
	private int mNumRowsSelected;
	private int mCurScanShaInt;
	private String mCurScanShaStr;
    private long startingPrimaryKey;
    private Connection connection = null;
	
    /*
     * WorkerSelect constructor
     */
	WorkerSelect(SelectQueriesPerSecond qps, int threadId) {
		// Half the workers will select with a conditional on SCAN_SHA
		// The remaining half will select on SCAN_ID
		workerType = (threadId < (SelectIbiUnion.NUM_WORKERS / 2)) ? SelectIbiUnion.WORKER_TYPE_SCAN_ID :
			SelectIbiUnion.WORKER_TYPE_SCAN_SHA;
		mQps = qps;
		mThreadId = String.format("%03d", threadId);
		mQueryStartTime = 0;
		mQueryTime = 0;
		mNumRowsSelected = 0;
		mCurScanShaInt = 0;
		mCurScanShaStr = String.format("%08d", mCurScanShaInt);
		startingPrimaryKey = Long.MAX_VALUE / SelectIbiUnion.NUM_WORKERS * threadId;
	}
	
	/*
	 * run method. Opens a connection to MemSQL, prepares a SELECT statement,
	 * then calls that statement over and over for a defined number of times,
	 * changing SCAN_ID or SCAN_SHA every query, depending on the worker type.
	 * The number of queries per second is printed for the thread.
	 */
	public void run() {
        try {
        	connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/memsql", "root", "");
		} catch (SQLException e) {
			System.out.println("Failed to make connection!");
			e.printStackTrace();
		}

        try {
        	String sql = (workerType == SelectIbiUnion.WORKER_TYPE_SCAN_ID) ?
        			"SELECT * FROM MemEx.IBI_UNION WHERE SCAN_ID = ?" :
        				"SELECT * FROM MemEx.IBI_UNION WHERE SCAN_SHA = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);

        	for (int i = 0; i < SelectIbiUnion.NUM_QUERIES_PER_WORKER; i++) {
        		if (workerType == SelectIbiUnion.WORKER_TYPE_SCAN_ID) {
        			stmt.setLong(1, startingPrimaryKey++);
        		} else {
        			stmt.setString(1, mThreadId + mCurScanShaStr);
    				if (++mCurScanShaInt > SelectIbiUnion.MAX_WORKER_SCAN_SHA) {
    					mCurScanShaInt = 0;
    				}
    				mCurScanShaStr = String.format("%08d", mCurScanShaInt);
        		}

        		mQueryStartTime = System.nanoTime();
        		if (stmt.execute()) {
        			mQueryTime += System.nanoTime() - mQueryStartTime;
        			mNumRowsSelected++;
        		}
        		
        		Thread.sleep(1); // Sleep for 1/1000th of a second
        	}
        } catch (SQLException e) {
			System.out.println("Failed to execute select statement!");
			e.printStackTrace();
        } catch (InterruptedException e) {
        	System.out.println("Sleep interrupted!");
			e.printStackTrace();
		}
        
        try {
        	connection.close();
        } catch (SQLException e) {
			System.out.println("Failed to close connection!");
			e.printStackTrace();
		}
        
        double workerQps = mNumRowsSelected / ((double) (mQueryTime / 1000000000));
        if (workerType == SelectIbiUnion.WORKER_TYPE_SCAN_ID) {
        	mQps.addScanIdQps((int) workerQps);
        } else {
        	mQps.addScanShaQps((int) workerQps);
        }
        System.out.println("Worker" + mThreadId + "      " + workerQps + "      " + workerType + "      " + mNumRowsSelected + "      " + mQueryTime);
	}
}

/**
 * SelectQueriesPerSecond is an atomic counter that can be used by each
 * WorkerSelect thread concurrently.
 * 
 * @author andrewfisher
 */
class SelectQueriesPerSecond {
	private AtomicInteger scanShaQps = new AtomicInteger(0);
	private AtomicInteger scanIdQps = new AtomicInteger(0);
	
	/*
	 * addScanShaQps atomically adds the given queries / second number to the current
	 * value.
	 */
	public void addScanShaQps(int qps) {
		this.scanShaQps.addAndGet(qps);
	}
	
	/*
	 * getQps returns the number of queries / second.
	 */
	public int getScanShaQps() {
		return scanShaQps.get();
	}
	
	/*
	 * addScanIdQps atomically adds the given queries / second number to the current
	 * value.
	 */
	public void addScanIdQps(int qps) {
		this.scanIdQps.addAndGet(qps);
	}
	
	/*
	 * getScanIdQps returns the number of queries / second.
	 */
	public int getScanIdQps() {
		return scanIdQps.get();
	}
}
