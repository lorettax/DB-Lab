package tinydb.systemtest;

import org.junit.Before;

import tinydb.common.Database;

public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
