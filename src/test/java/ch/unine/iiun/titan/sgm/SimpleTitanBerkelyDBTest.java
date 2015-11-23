/**
 * Author: Valerio Schiavoni <valerio.schiavoni@gmail.com>
 */
package ch.unine.iiun.titan.sgm;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * 
 */
public class SimpleTitanBerkelyDBTest {
	TitanGraph berkelyje;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.berkelyje = TitanFactory.open("berkeleyje:/tmp/graph");
		assertNotNull(berkelyje);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		try {
			berkelyje.close();
		} catch (Exception e) {
			fail("can't close berkeleyje");
		}

	}

	@Test
	public void test() {
		fail("Not yet implemented");
	}

}
