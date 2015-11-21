/**
 * Author: Valerio Schiavoni <valerio.schiavoni@gmail.com>
 */
package ch.unine.iiun.titan.sgm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * Mostly rough tests to play with the TitanDB Java APIs, nothing smart going on
 * here.
 * 
 */
public class SimpleTitanTest {

	/**
	 * Total number of edges in the WikiVote graph.
	 */
	private static final double WIKIVOTE_TOTAL_EDGES = 103689.0;
	TitanGraph inmemory;
	TitanGraph berkelyje;

	/**
	 * @see com.thinkaurelius.titan.graphdb.TitanGraphBaseTest#setUp()
	 */
	@Before
	public void setUp() throws Exception {
		this.inmemory = TitanFactory.open("inmemory");
		this.berkelyje = TitanFactory.open("berkeleyje:/tmp/graph");

		assertNotNull(inmemory);
		assertNotNull(berkelyje);
	}

	@Test
	public void testAddVertexInMemory() {
		final TitanVertex v = inmemory.addVertex("roger");
		v.property("name", "roger");
		inmemory.tx().commit();
		// how to test for available nodes?
	}

	@Test
	public void testPopulateInMemoryGraphWikiVote()
			throws FileNotFoundException, IOException {
		TitanManagement mgmt = inmemory.openManagement();
		mgmt.makePropertyKey("name").dataType(String.class);
		mgmt.commit();

		loadWikiVote(inmemory);

	}

	@Test
	public void testPopulateBatchLoading()
			throws FileNotFoundException, IOException {
		BaseConfiguration bc = new BaseConfiguration();
		bc.setProperty("storage.backend", "inmemory");
		// bc.setProperty("storage.directory", "/tmp/10m");
		bc.setProperty("storage.batch-loading", true);

		TitanGraph inmemoryBatch = TitanFactory.open(bc);
		assertNotNull(inmemoryBatch);
		TitanManagement mgmt = inmemoryBatch.openManagement();
		mgmt.makePropertyKey("name").dataType(String.class);
		mgmt.commit();
		loadWikiVote(inmemoryBatch);

	}

	@After
	public void tearDown() {
		try {
			berkelyje.close();
		} catch (Exception e) {
			fail("can't close berkeleyje");
		}

		try {
			inmemory.close();
		} catch (Exception e) {
			fail("can't close inmemory");
		}

	}

	private void loadWikiVote(TitanGraph gr)
			throws FileNotFoundException, IOException {
		int edgesCounter = 0;

		long start = System.currentTimeMillis();

		File f = new File("graphs/wiki-Vote.txt");
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			for (String line; (line = br.readLine()) != null;) {
				if (!line.startsWith("#")) {
					// System.out.println(line);
					final String[] edgeVertices = line.split("\t");
					Vertex fromVertex = getOrCreate(inmemory, edgeVertices[0]);
					Vertex dest = getOrCreate(inmemory, edgeVertices[1]);
					fromVertex.addEdge("votesFor", dest);
					// System.out.println("Added edges:" + edgesCounter++);
					edgesCounter++;
					if (edgesCounter % 100 == 0) {
						String progress = String.format("%.2f",
								edgesCounter / WIKIVOTE_TOTAL_EDGES) + " %";

						System.out.println("Added edges:" + edgesCounter + " "
								+ durationSince(start,
										System.currentTimeMillis())
								+ " " + progress);
					}
				}

			}

		}
		long stop = System.currentTimeMillis();

		System.out.println("Total edges:" + edgesCounter);
		System.out.println("Elapsed time: " + durationSince(start, stop));

		assertEquals((int) WIKIVOTE_TOTAL_EDGES, edgesCounter);
	}

	private String durationSince(long start, long stop) {
		Duration duration = new Duration(start, stop);

		PeriodFormatter formatter = new PeriodFormatterBuilder().appendDays()
				.appendSuffix("d").appendHours().appendSuffix("h")
				.appendMinutes().appendSuffix("m").appendSeconds()
				.appendSuffix("s").toFormatter();
		String formatted = formatter.print(duration.toPeriod());

		return formatted;
	}

	private Vertex getOrCreate(TitanGraph g, String vertexId) {
		Iterator<Vertex> vertices = g.vertices();
		if (!vertices.hasNext()) { // empty graph?
			Vertex v = g.addVertex("id", vertexId);
			return v;
		} else
			while (vertices.hasNext()) {
				Vertex nextVertex = vertices.next();
				if (nextVertex.property("id").equals(vertexId)) {
					return nextVertex;
				} else {
					Vertex v = g.addVertex("id", vertexId);
					return v;
				}
			}
		return null;
	}
}
