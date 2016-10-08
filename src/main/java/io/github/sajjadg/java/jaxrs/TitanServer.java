package io.github.sajjadg.java.jaxrs;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import io.github.sajjadg.java.jaxrs.resource.TitanService;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.UriBuilder;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.structure.T.label;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;


public final class TitanServer {

    private final String SERVER_IP = "http://0.0.0.0";
    private final int SERVER_PORT = 8090;
    public static TitanGraph graph;
    public static GraphTraversalSource g;
    public static ArrayList<String> props;

    static {
        SugarLoader.load(); //load syntax sugar plugin
    }

    /**
     * Server constructor which initialize the graph and rest service
     */
    public TitanServer() {

        // Initialize graph db
        cleanGraph();
        populateGraph();
        initGraphDB();
        System.out.println("DB is up and ready for getting requests");

        System.out.println(IteratorUtils.count(graph.vertices()));
        System.out.println(IteratorUtils.count(graph.edges()));

        //load configs from /etc/ratserver.conf file
        String configPath = "/etc/ratserver.conf";
        try {
            System.out.println("Configuration loaded ...");
            File f = new File(configPath);
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(f)));
//            Configs.load(properties);
            System.out.println("Configuration loaded from " + f.getAbsolutePath());
        } catch (IOException ex) {
            System.err.println("NO CONFIG FILE FOUND AT " + configPath);
            Logger.getLogger(TitanServer.class.getName()).log(Level.SEVERE, null, ex);
//            System.exit(1);
        }

        //-----------------------------------------------------------------
        //Run REST server to handle requests
        //-----------------------------------------------------------------
        try {
            URI baseUri = UriBuilder.fromUri(SERVER_IP).port(SERVER_PORT).build();
            final ResourceConfig rc = new ResourceConfig();
            rc.register(CORSFilter.class);
            rc.register(MultiPartFeature.class);
            rc.register(GsonJerseyProvider.class);
            rc.register(TitanService.class);

            //TODO: secure service will SSL
//            HashSet<NetworkListener> lists=new HashSet<NetworkListener>(secure.getListeners());
//            for (NetworkListener listener : lists){
//                listener.setSecure(true);
//                SSLEngineConfigurator ssle=new SSLEngineConfigurator(sslCon);
//                listener.setSSLEngineConfig(ssle);
//                secure.addListener(listener);
//                System.out.println(listener);
//            }
//            httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, rc, true, createSSLConfig(false, false, false));

            HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, rc);
            System.out.println("-> [INFO] Server started handling RESTful requests [" + baseUri.toString() + "]");

        } catch (IllegalArgumentException | NullPointerException e) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, e);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     *
     */
    public void initGraphDB() {
        System.out.println("initializing the graph");
        graph = TitanFactory.open("cassandra:localhost");
        g = graph.traversal();
    }

    /**
     * Delete all data from DB
     */
    public void cleanGraph() {
        System.out.println("Cleaning the graph");
        graph = TitanFactory.open("cassandra:localhost");
        graph.close();//shutdown
        TitanCleanup.clear(graph);
    }

    /**
     * Inserts a sample property graph into the DB
     * TODO: load database from file
     */
    public void populateGraph() {
        System.out.println("Populating the graph");
        graph = TitanFactory.open("cassandra:localhost");
        g = graph.traversal();

        int st_id = 95111001;
        int age = 18;
        double avg = 10.10;

        props = new ArrayList<>();
        props.add("st_id");
        props.add("name");
        props.add("lname");
        props.add("age");
        props.add("avg");
        props.add("born_city");
        props.add("born_province");
        props.add("living_city");
        props.add("living_province");
        props.add("study_city");
        props.add("study_province");

        ArrayList<TitanVertex> titanVertices = new ArrayList<>();

        TitanVertex v1 = graph.addVertex(label, "student");
        v1.property("st_id", st_id++);
        v1.property("name", "Ali");
        v1.property("lname", "Rajabi");
        v1.property("age", age++);
        v1.property("avg", avg++);
        v1.property("born_city", "Shiraz");
        v1.property("born_province", "Shiraz");
        v1.property("living_province", "Qom");
        v1.property("living_city", "Qom");
        v1.property("study_province", "Qom");
        v1.property("study_city", "Qom");


        TitanVertex v4 = graph.addVertex(label, "student");
        v4.property("st_id", st_id++);
        v4.property("name", "Saeed");
        v4.property("lname", "Gerami");
        v4.property("age", age++);
        v4.property("avg", avg++);
        v4.property("born_city", "Lar");
        v4.property("born_province", "Shiraz");
        v4.property("living_city", "Qom");
        v4.property("living_province", "Qom");
        v4.property("study_city", "Qom");
        v4.property("study_province", "Qom");

        TitanVertex v6 = graph.addVertex(label, "student");
        v6.property("st_id", st_id++);
        v6.property("name", "Ahmad");
        v6.property("lname", "Ghorbani");
        v6.property("age", age++);
        v6.property("avg", avg++);
        v6.property("born_city", "Isfahan");
        v6.property("born_province", "Isfahan");
        v6.property("living_city", "Qom");
        v6.property("living_province", "Qom");
        v6.property("study_city", "Qom");
        v6.property("study_province", "Qom");

        TitanVertex v2 = graph.addVertex(label, "student");
        v2.property("st_id", st_id++);//"95111002"
        v2.property("name", "Mohammad");
        v2.property("lname", "Morshedi");
        v2.property("age", age++);
        v2.property("avg", avg++);
        v2.property("born_city", "Mashhad");
        v2.property("born_province", "Mashhad");
        v2.property("living_city", "Tehran");
        v2.property("living_province", "Tehran");
        v2.property("study_city", "city1");
        v2.property("study_province", "Mashhad");

        TitanVertex v3 = graph.addVertex(label, "student");
        v3.property("st_id", st_id++);
        v3.property("name", "Ghasem");
        v3.property("lname", "Esmaeili");
        v3.property("age", age++);
        v3.property("avg", avg++);
        v3.property("born_city", "Tabriz");
        v3.property("born_province", "West Azarbayejan");
        v3.property("living_city", "Tehran");
        v3.property("living_province", "Tehran");
        v3.property("study_city", "Kerman");
        v3.property("study_province", "Kerman");

        TitanVertex v7 = graph.addVertex(label, "student");
        v7.property("st_id", st_id++);
        v7.property("name", "Jafar");
        v7.property("lname", "Gholami");
        v7.property("age", age++);
        v7.property("avg", avg++);
        v7.property("born_city", "Isfahan");
        v7.property("born_province", "Isfahan");
        v7.property("living_city", "Qom");
        v7.property("living_province", "Qom");
        v7.property("study_city", "Qom");
        v7.property("study_province", "Qom");

        TitanVertex v5 = graph.addVertex(label, "student");
        v5.property("st_id", st_id++);
        v5.property("name", "AliAkbar");
        v5.property("lname", "Gholami");
        v5.property("age", age++);
        v5.property("avg", avg++);
        v5.property("born_city", "Rasht");
        v5.property("born_province", "Mazandaran");
        v5.property("living_city", "Qom");
        v5.property("living_province", "Qom");
        v5.property("study_city", "Qom");
        v5.property("study_province", "Qom");


        titanVertices.add(v1);
        titanVertices.add(v2);
        titanVertices.add(v3);
        titanVertices.add(v4);
        titanVertices.add(v5);
        titanVertices.add(v6);
        titanVertices.add(v7);

        for (int i = 0; i < 7; i++) {
            for (int j = 0; j < 7; j++) {
                if (i != j)
                    titanVertices.get(i).addEdge("friend", titanVertices.get(j));
            }
        }

        //TODO: create index for 'name'

        //Prints friends of Saeed
        GraphTraversal<Vertex, Vertex> friends = g.V().has("name", "Saeed").out("friend");
        while (friends.hasNext()) {
            printValues(friends.next());
        }

        System.out.println(IteratorUtils.count(graph.vertices()));
        System.out.println(IteratorUtils.count(graph.edges()));

        graph.tx().commit();
        graph.close();//shutdown
    }

    /**
     * Prints the properties of a vertex
     *
     * @param vertex
     */
    public static void printValues(Vertex vertex) {
        System.out.println("-------------------------------------");
        for (String p : props) {
            System.out.println("\t" + vertex.property(p));
        }
    }

    /**
     * Main method which runs the server
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        new TitanServer();
    }
}
