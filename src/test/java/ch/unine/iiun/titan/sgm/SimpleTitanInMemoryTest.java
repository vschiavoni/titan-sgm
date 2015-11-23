/**
 * Author: Valerio Schiavoni <valerio.schiavoni@gmail.com>
 */
package ch.unine.iiun.titan.sgm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;

/**
 * Mostly rough tests to play with the TitanDB Java APIs, nothing smart going on
 * here.
 * 
 */
public class SimpleTitanInMemoryTest {

	/**
	 * Total number of edges in the WikiVote graph.
	 */
	private static final double WIKIVOTE_TOTAL_EDGES = 103689.0;
	TitanGraph inmemory;
	LineConsumer lc;

	/**
	 * @see com.thinkaurelius.titan.graphdb.TitanGraphBaseTest#setUp()
	 */
	@Before
	public void setUp() throws Exception {
		this.inmemory = TitanFactory.open("inmemory");
		this.lc = new LineConsumer(inmemory);
		assertNotNull(inmemory);

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
		mgmt.makePropertyKey("vertexId").dataType(String.class)
				.cardinality(Cardinality.SINGLE).make();

		mgmt.buildIndex("byVertexId", Vertex.class)
				.addKey(mgmt.getOrCreatePropertyKey("vertexId"))
				.buildCompositeIndex();

		mgmt.commit();

		try { // wait 'byVertexId' index
			ManagementSystem.awaitGraphIndexStatus(inmemory, "byVertexId")
					.status(SchemaStatus.ENABLED).call();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

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
		mgmt.makePropertyKey("vertexId").dataType(String.class)
				.cardinality(Cardinality.SINGLE).make();
		mgmt.commit();
		loadWikiVote(inmemoryBatch);

	}

	@After
	public void tearDown() {

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

		Path path = Paths.get("graphs/", "wiki-Vote.txt");
		// The stream hence file will also be closed here
		try (Stream<String> lines = Files.lines(path)) {
			lines.filter(s -> !s.startsWith("#")).forEach(this.lc);
		}
		System.out.println("added all edges");
		// edgesCounter++;

		// String progress = String.format("%.2f",
		// edgesCounter / WIKIVOTE_TOTAL_EDGES) + " %";

		// System.out.println("Added edges:" + edgesCounter + " "
		// + Utils.durationSince(start, System.currentTimeMillis()) + " "
		// + progress);

		long stop = System.currentTimeMillis();

		System.out.println("Total edges:" + edgesCounter);
		System.out.println("Elapsed time: " + Utils.durationSince(start, stop));

		assertEquals((int) WIKIVOTE_TOTAL_EDGES, edgesCounter);
	}

	private class LineConsumer implements Consumer<String> {

		private final TitanGraph g;

		public LineConsumer(TitanGraph g) {
			this.g = g;
		}

		@Override
		public void accept(String t) {
			final String[] edgeVertices = t.split("\t");
			Vertex fromVertex = getOrCreate(this.g, edgeVertices[0]);
			Vertex dest = getOrCreate(this.g, edgeVertices[1]);
			fromVertex.addEdge("votesFor", dest);
		}

	}

	private Vertex getOrCreate(TitanGraph graph, String vertexId) {

		GraphTraversalSource g = graph.traversal();
		return g.V().has("uniqueId", vertexId).tryNext()
				.orElseGet(() -> graph.addVertex("uniqueId", vertexId));

	}
}
