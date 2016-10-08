package playground;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.structure.T.label;

/**
 * Created by ironman on 9/30/16.
 */
public class TitanTest {
    public static void main(String[] args) {

//        test2();
        test3();

    }

    private static void test3() {

//        TinkerGraph graph = TitanFactory.open("cassandra:localhost");
        TinkerGraph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();

        GraphTraversal<Vertex, Double> values = g.V().has("name", within("vadas", "marko")).values("age").mean();

        try {

            System.out.println(values.next().toString());
//            System.out.println("--" + values.next().toString());
            GraphTraversal<Vertex, String> values1 = g.V().has("name", "marko").out("created").in("created").values("name");
//            System.out.println(values1.toList().size());
            System.out.println("+++++" + values1.next());
            for (String s : values1.toList()) {
                System.out.println("++" + s);
            }

            System.out.println("====");

            GraphTraversal<Vertex, String> values2 = g.V().has("name", "marko").out("created").in("created").values("name");
            System.out.println(values2.next());

        } catch (Exception e) {
            e.printStackTrace();
        }

        GraphTraversal<Vertex, String> values3 = g.V().has("name", "marko").as("exclude").out("created").in("created").where(neq("exclude")).values("name");
        for (String s : values3.toList()) {
            System.out.println("-=-" + s);
        }

        List<Map<Object, Object>> names = g.V().group().by(label).by("name").toList();
        for (Map<Object, Object> s : names) {
            System.out.println("-=-==" + s.get("name"));
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static void test2() {
        TitanGraph graph = TitanFactory.open("cassandra:localhost");

        graph.vertices();

        graph.close();//shutdown
        TitanCleanup.clear(graph);//TODO:remove this

        graph = TitanFactory.open("cassandra:localhost");

        TitanVertex v1 = graph.addVertex(label, "person", "name", "marko", "age", 29);
        TitanVertex v2 = graph.addVertex(label, "software", "name", "lop", "lang", "java");
        v1.addEdge("created", v2, "weight", 0.4);

        GraphTraversalSource g = graph.traversal();
        GraphTraversal<Vertex, Vertex> has = g.V().has("name", "marko").outE("created").inV();
        GraphTraversal<Vertex, String> values = g.V().has("name", "marko").out("created").values("name");
        System.out.println(values.next().toString());

        graph.close();

    }

    public void test(String[] args) {
        BaseConfiguration baseConfiguration = new BaseConfiguration();
        baseConfiguration.setProperty("storage.backend", "cassandra");
        baseConfiguration.setProperty("storage.hostname", "127.0.0.1");

        SugarLoader.load();

//        TitanGraph titanGraph = TitanFactory.open(baseConfiguration);
        TitanGraph graph = TitanFactory.open("cassandra:localhost");

        graph.vertices();

        graph.close();//shutdown
        TitanCleanup.clear(graph);//TODO:remove this

        graph = TitanFactory.open("cassandra:localhost");


        TitanManagement management = graph.openManagement();
        final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
        TitanGraphIndex namei = management.buildIndex("name", Vertex.class).addKey(name).unique().buildCompositeIndex();
        management.setConsistency(namei, ConsistencyModifier.LOCK);

        final PropertyKey time = management.makePropertyKey("time").dataType(Integer.class).make();
        TitanGraphIndex timei = management.buildIndex("time", Edge.class).addKey(time).buildCompositeIndex();
        management.setConsistency(timei, ConsistencyModifier.LOCK);

        management.makeVertexLabel("Customer").make();
        management.makeVertexLabel("Shipment").make();
        management.makeVertexLabel("Piece").make();
        management.makeVertexLabel("Pallet").make();
        management.makeEdgeLabel("orders").make();
        management.makeEdgeLabel("consists-of").make();
        management.makeEdgeLabel("is-bundled-on").make();

        management.commit();

        System.out.println("--------------- DONE --------------");

//        graph.addVertex();

        Vertex gremlin = graph.addVertex(label, "software", "name", "gremlin");
        // add a new property
        gremlin.property("created", 2009);

        // only one vertex should exist
        assert (IteratorUtils.count(graph.vertices()) == 1);
        // no edges should exist as none have been created
        assert (IteratorUtils.count(graph.edges()) == 0);


        // add a new software vertex to the graph
        Vertex blueprints = graph.addVertex(label, "software", "name", "blueprints");

        // connect gremlin to blueprints via a dependsOn-edge
        gremlin.addEdge("dependsOn", blueprints);

        // now there are two vertices and one edge
        assert (IteratorUtils.count(graph.vertices()) == 2);
        assert (IteratorUtils.count(graph.edges()) == 1);

        // add a property to blueprints
        blueprints.property("created", 2010);
//        blueprints.property("name", "blueprint");

        // remove that property
        blueprints.property("created").remove();

        // connect gremlin to blueprints via encapsulates
        gremlin.addEdge("encapsulates", blueprints, "weight", 1.0d);
        assert (IteratorUtils.count(graph.vertices()) == 2);
        assert (IteratorUtils.count(graph.edges()) == 2);

        // removing a vertex removes all its incident edges as well
//        blueprints.remove();
//        gremlin.remove();

        // the graph is now empty
        assert (IteratorUtils.count(graph.vertices()) == 0);
        assert (IteratorUtils.count(graph.edges()) == 0);

        System.out.println("tada!");

//        TitanGraphQuery query = graph.query().has("gremlin");
//        query.


        Vertex rash;
        rash = graph.addVertex("lable1");
        rash.property("userId", 1);
        rash.property("username", "rash");
        rash.property("name", "Rahul");
        rash.property("lastName", "Chaudhary");
        rash.property("birthday", 101);

        Vertex honey = graph.addVertex("lable2");
        honey.property("userId", 2);
        honey.property("username", "honey");
        honey.property("name", "Honey");
        honey.property("lastName", "Anant");
        honey.property("birthday", 201);

//        Edge frnd = graph.addEdge(null, rash, honey, "FRIEND");
        rash.addEdge("friend", honey, "since", 2011);

//        graph.commit();

//        Iterable<Vertex> results = rash.query().labels("FRIEND").has("since", 2011).vertices();
//        Iterable<TitanVertex> results = graph.query().has("since", 2011).vertices();
//        Iterable<TitanVertex> results = graph.query().has("dependsOn", blueprints).vertices();
//        Iterable<TitanVertex> results = graph.query().has("friend").vertices();
//        Iterable<TitanVertex> results = graph.query().has("dependsOn", blueprints).vertices();
//
//        for (Vertex result : results) {
//            System.out.println("Id: " + result.property("userId"));
//            System.out.println("Username: " + result.property("username"));
//            System.out.println("Name: " + result.property("firstName") + " " + result.property("lastName"));
//        }

        System.out.println(listVertices(graph));

        System.out.println(IteratorUtils.count(graph.vertices()));
        System.out.println(IteratorUtils.count(graph.edges()));

        GraphTraversalSource g = graph.traversal();
//        System.out.println(g.V.out.name);
        GraphTraversal<Vertex, Vertex> vertexVertexGraphTraversal = g.V("userId", 1).outE("friend").inV();
        if (vertexVertexGraphTraversal.hasNext()) {
            Vertex next = vertexVertexGraphTraversal.next();
            System.out.println("-----------" + next.property("name"));
        }
        List<Vertex> friend = g.V().out("friend").toList();
        for (Vertex vertex : friend) {
            System.out.println("+++++++++++" + vertex.property("name"));
        }

        graph.close();
    }

    public static String listVertices(TitanGraph g) {
        List<String> gods = new ArrayList<String>();
        Iterator<Vertex> itty = g.vertices();
        Vertex v;
        while (itty.hasNext()) {
            v = itty.next();
            gods.add((String) v.property("name").value());
        }
        return gods.toString();
    }

    public static String listVertices(Iterator<Vertex> itty) {
        List<String> gods = new ArrayList<String>();
//        Iterator<Vertex> itty = g.vertices();
        Vertex v;
        while (itty.hasNext()) {
            v = itty.next();
            gods.add((String) v.property("name").value());
        }
        return gods.toString();
    }

}
