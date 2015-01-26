import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.*;

/**
 * InsertIbiUnion is a multithreaded JAVA program that executes an INSERT
 * statement into the IBI_UNION database table. It determines the number of
 * inserts per second using MemSQL.
 * 
 * TODO: Allow one to stop and restart this program without 1) truncating
 * the entire IBI_UNION database or 2) modifying the worker thread logic
 * to determine what primary key to start inserting at. 
 * 
 * @author andrewfisher
 */
public class InsertIbiUnion {
	/*
	 * NUM_WORKERS defines the total number of threads that will be used in this
	 * benchmarking test of MemSQL.
	 */
	public static final int NUM_WORKERS = 50;

	/*
	 * NUM_QUERIES_PER_WORKER is how many inserts each thread should complete
	 * before ending.
	 */
	public static final int NUM_QUERIES_PER_WORKER = 10000;

	/*
	 * SCANS_PER_PACKAGE will be used to calculate the SCAN_SHA for added
	 * records. No more than seven inserted records should have the same
	 * SCAN_SHA.
	 */
	public static final int SCANS_PER_PACKAGE = 7;

	/*
	 * Since the SCAN_SHA column is VARCHAR(11), the first three digits of the
	 * SCAN_SHA for any added record will be the three digit thread ID followed
	 * by an eight digit zero padded number that cannot exceed 9,999,999.
	 */
	public static final int MAX_WORKER_SCAN_SHA = 99999999;

	/*
	 * The following constants will be used to fill the VARCHAR, TIMESTAMP, and
	 * DATE columns for inserted records into IBI_UNION except for SCAN_ID and
	 * SCAN_SHA.
	 */
	public static final String DEFAULT_SCAN_TYPE = "aaa";
	public static final int DEFAULT_SCAN_COUNT = 0;
	public static final String DEFAULT_MACHINE_TYPE = "aaaaaaaaaa";
	public static final String DEFAULT_SEQUENCE_NBR = "aaaaa";
	public static final Timestamp DEFAULT_TIMESTAMP = new Timestamp(
			System.currentTimeMillis());
	public static final Date DEFAULT_DATE = new Date(System.currentTimeMillis());

	/*
	 * main method instantiates WorkerInsert threads and starts them. A
	 * WorkerInsert thread performs benchmarking of MemSQL. At the very
	 * end it prints the total number of inserts per second.
	 */
	public static void main(String[] args) throws InterruptedException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		}
		
		final QueriesPerSecond mQps = new QueriesPerSecond();
		
		// create worker threads
		ArrayList<WorkerInsert> workers = new ArrayList<WorkerInsert>();
		for (int i = 0; i < NUM_WORKERS; i++) {
			workers.add(new WorkerInsert(mQps, i));
		}
		
        System.out.println("  Thread:       Queries / Second:");

		// start all the workers
		Iterator<WorkerInsert> i1 = workers.iterator();
		while (i1.hasNext()) {
			WorkerInsert wi = (WorkerInsert) i1.next();
			wi.start();
		}
		
		// wait for all the workers to finish
		Iterator<WorkerInsert> i2 = workers.iterator();
		while (i2.hasNext()) {
			WorkerInsert wi = (WorkerInsert) i2.next();
			wi.join();
		}
		
        System.out.println("-------------------------");
        System.out.println("   TOTAL:       " + mQps.getQps());
	}
}

/**
 * WorkerInsert is a thread that connects to MemSQL, defines a prepared INSERT
 * SQL statement, then executes the query for NUM_QUERIES_PER_WORKER times. At
 * the end it prints the number of inserts per second for the thread. There 
 * are trivial JAVA operations included in the final benchmark time, but the
 * assumption is that they are immaterial.
 * 
 * @author andrewfisher
 */
class WorkerInsert extends Thread {
	private QueriesPerSecond mQps;
	private String mThreadId;
	private long mQueryStartTime;
	private long mQueryTime;
	private int mNumRowsInserted;
	private int mCurScanShaInt;
	private String mCurScanShaStr;
    private long startingPrimaryKey;
    private Connection connection = null;
	
    /*
     * WorkerInsert constructor
     */
	WorkerInsert(QueriesPerSecond qps, int threadId) {
		mQps = qps;
		mThreadId = String.format("%03d", threadId);
		mQueryStartTime = 0;
		mQueryTime = 0;
		mNumRowsInserted = 0;
		mCurScanShaInt = 0;
		mCurScanShaStr = String.format("%08d", mCurScanShaInt);
		startingPrimaryKey = (Long.MAX_VALUE / InsertIbiUnion.NUM_WORKERS * threadId) + 500001;
	}
	
	/*
	 * run method. Opens a connection to MemSQL, prepares an INSERT statement,
	 * then calls that statement over and over for a defined number of times,
	 * changing SCAN_ID every time and SCAN_SHA every seven times. The number of
	 * queries per second is printed for the thread.
	 */
	public void run() {
        try {
        	connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/memsql", "root", "");
		} catch (SQLException e) {
			System.out.println("Failed to make connection!");
			e.printStackTrace();
			return;
		}
        
        try {
        	Statement stmt = connection.createStatement();
        	stmt.execute("USE MemEx");
        	
        	PreparedStatement insertStmt = connection.prepareStatement(
        			"INSERT INTO IBI_UNION ("
        			+ "SCAN_ID,"
        			+ "SCAN_SHA,"
        			+ "SCAN_TYPE,"
        			+ "SCAN_COUNT,"
        			+ "MACHINE_TYPE,"
        			+ "SEQUENCE_NBR,"
        			+ "IOS_LOAD_DATE,"
        			+ "MPT_LOAD_DATE,"
        			+ "LOAD_DATE) "
        			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        	
    		insertStmt.setString(3, InsertIbiUnion.DEFAULT_SCAN_TYPE);
    		insertStmt.setInt(4, InsertIbiUnion.DEFAULT_SCAN_COUNT);
    		insertStmt.setString(5, InsertIbiUnion.DEFAULT_MACHINE_TYPE);
    		insertStmt.setString(6, InsertIbiUnion.DEFAULT_SEQUENCE_NBR);
    		insertStmt.setTimestamp(7, InsertIbiUnion.DEFAULT_TIMESTAMP);
    		insertStmt.setTimestamp(8, InsertIbiUnion.DEFAULT_TIMESTAMP);
    		insertStmt.setDate(9, InsertIbiUnion.DEFAULT_DATE);

    		mQueryStartTime = System.nanoTime();
        	for (int i = 0; i < InsertIbiUnion.NUM_QUERIES_PER_WORKER; i++) {
        		insertStmt.setLong(1, startingPrimaryKey);
        		insertStmt.setString(2, mThreadId + mCurScanShaStr);
        		
        		if (insertStmt.executeUpdate() == 1) {
        			mNumRowsInserted++;
        			startingPrimaryKey++;
        			if ((mNumRowsInserted % InsertIbiUnion.SCANS_PER_PACKAGE) == 0) {
        				if (++mCurScanShaInt > InsertIbiUnion.MAX_WORKER_SCAN_SHA) {
        					mCurScanShaInt = 0;
        				}
        				mCurScanShaStr = String.format("%08d", mCurScanShaInt);
        			}
        		}
        	}
			mQueryTime = System.nanoTime() - mQueryStartTime;
        } catch (SQLException e) {
			System.out.println("Failed to change database, prepare statement, or execute prepared statement!");
			e.printStackTrace();
			return;
        }
        
        try {
        	connection.close();
        } catch (SQLException e) {
			System.out.println("Failed to close connection!");
			e.printStackTrace();
			return;
		}
        
        int workerQps = Math.round(mNumRowsInserted / (mQueryTime / 1000000000));
        mQps.addQps(workerQps);
        System.out.println("Worker" + mThreadId + "      " + workerQps);
	}
}

/**
 * QueriesPerSecond is an atomic counter that can be used by each WorkerInsert
 * thread concurrently.
 * 
 * @author andrewfisher
 */
class QueriesPerSecond {
	private AtomicInteger qps = new AtomicInteger(0);
	
	/*
	 * addQps atomically adds the given queries / second number to the current
	 * value.
	 */
	public void addQps(int qps) {
		this.qps.addAndGet(qps);
	}
	
	/*
	 * getQps returns the number of queries / second.
	 */
	public int getQps() {
		return qps.get();
	}
}
