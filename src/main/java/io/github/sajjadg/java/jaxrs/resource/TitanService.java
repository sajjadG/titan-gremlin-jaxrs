package io.github.sajjadg.java.jaxrs.resource;

import com.clearspring.analytics.util.Pair;
import com.google.gson.Gson;
import com.thinkaurelius.titan.core.TitanGraph;
import io.github.sajjadg.java.jaxrs.TitanServer;
import static io.github.sajjadg.java.jaxrs.TitanServer.g;
import static io.github.sajjadg.java.jaxrs.TitanServer.graph;
import io.github.sajjadg.java.jaxrs.model.Student;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.structure.Direction.BOTH;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * http://sql2gremlin.com/ graph.traversal() should be "global" to all your
 * traversals. Its a thread-safe object that provides you traversals. You should
 * never have in your code, over and over again, graph.traversal().V(). Creating
 * a graph.traversal() is expensive, using it over and over is cheap.
 */
@Path("/graph")
public class TitanService {

    /**
     * Find the closest friend with average to a student
     *
     * @param name Student name
     * @param limit
     * @return
     */
    @GET
    @Path("/similar/{name}/{limit}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response similarStudent(@PathParam("name") String name, @PathParam("limit") int limit) {

        System.out.println(IteratorUtils.count(TitanServer.graph.vertices()));
        System.out.println(IteratorUtils.count(TitanServer.graph.edges()));
        try {

            GraphTraversalSource g = TitanServer.graph.traversal();

            //find our student average
            ArrayList<Double> avgList = (ArrayList<Double>) g.V().hasLabel("student").has("name", name).valueMap("avg").next().get("avg");
            double avg = avgList.get(0);
            System.out.println("our avg =" + avg);

            //find the nearest student in avg to him/her
            //TODO: We can do it by a gremlin query using order by with a comparator I think. http://sql2gremlin.com/#_recommendation
            //http://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            //http://tinkerpop.apache.org/docs/3.0.0.M8-incubating/#order-step
            GraphTraversal<Vertex, Map<String, Object>> vertexMapGraphTraversal = g.V().has("name", name).out("friend").valueMap();
            ArrayList<Pair<Double, Student>> detailArrayList = new ArrayList<>();
            while (vertexMapGraphTraversal.hasNext()) {

                Student student = new Student();
                Map<String, Object> next = vertexMapGraphTraversal.next();

                ArrayList<Double> al = (ArrayList<Double>) next.get("avg");
                student.setAvg(al.get(0));

                ArrayList<Integer> age = (ArrayList<Integer>) next.get("age");
                student.setAge(age.get(0));

                ArrayList<String> nameArrayList = (ArrayList<String>) next.get("name");
                student.setName(nameArrayList.get(0));

                ArrayList<String> lnameArrayList = (ArrayList<String>) next.get("lname");
                student.setLname(lnameArrayList.get(0));

                ArrayList<String> studyCity = (ArrayList<String>) next.get("study_city");
                student.setStudyCity(studyCity.get(0));

                ArrayList<String> livingCity = (ArrayList<String>) next.get("living_city");
                student.setLivingCity(livingCity.get(0));

                ArrayList<String> bornCity = (ArrayList<String>) next.get("born_city");
                student.setBornCity(bornCity.get(0));

                Double ddd = Math.abs(student.getAvg() - avg);
                detailArrayList.add(new Pair<>(ddd, student));
                System.out.println("++++ " + next.get("name") + " " + next.get("lname") + " " + next.get("avg"));
            }

            for (int i = 0; i < detailArrayList.size(); i++) {
                for (int j = 0; j < detailArrayList.size() - 1; j++) {
                    if (detailArrayList.get(j).left > detailArrayList.get(i).left) {
                        Pair<Double, Student> tmpPair = detailArrayList.get(i);
                        detailArrayList.set(i, detailArrayList.get(j));
                        detailArrayList.set(j, tmpPair);
                    }
                }
            }

            ArrayList<Student> students = new ArrayList<>();
            for (Pair<Double, Student> pair : detailArrayList) {
                students.add(pair.right);
                System.out.println("^^^" + pair.left + " -> " + pair.right);
                System.out.println("" + new Gson().toJson(pair.right));
                if (--limit == 0) {
                    break;
                }
            }

            return Response.ok(students).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().build();
        }

    }

    private void printValues(Map<String, Object> next) {
        for (String key : next.keySet()) {
            System.out.print(key + "=" + next.get(key) + " ");
        }
        System.out.println();
    }

    /**
     * Finds friends of a student
     *
     * @param name Student name
     * @param properties Student properties to be return
     * @return Returns JSON properties of his/her friends
     */
    @GET
    @Path("/friend/{name}/{props}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response friends(@PathParam("name") String name, @PathParam("props") String properties) {

        System.out.println(IteratorUtils.count(TitanServer.graph.vertices()));
        System.out.println(IteratorUtils.count(TitanServer.graph.edges()));
        try {

            GraphTraversalSource g = TitanServer.graph.traversal();
//
//            TitanIndexQuery.Result<Vertex> vertexArrayList = (TitanIndexQuery.Result<Vertex>) TitanServer.graph.indexQuery("vertexByName", "v.text:(Saeed)").vertices();
//            System.out.println(vertexArrayList.getElement() + " " + vertexArrayList.getScore());

//            GraphTraversal<Vertex, Vertex> friends1 = TitanServer.g.V().has("name", "Saeed").out("friend");
//            while (friends1.hasNext()) {
////                printValues(friends1.next());
//                System.out.println(friends1.next().property("name") + " " + friends1.next().property("lname"));
//            }
//            GraphTraversal<Vertex, String> values = g.V().has("age", "20").values("name");
//            System.out.println(values.next());
//        GraphTraversal<Vertex, String> values = g.V().has("name", "marko").out("created").values("name");
            GraphTraversal<Vertex, Vertex> friends = TitanServer.g.V().has("name", name).out("friend");
//            System.out.println("result count=" + IteratorUtils.count(friends.count()));
            while (friends.hasNext()) {
//                System.out.println(friends.next().property("name") + " " + friends.next().property("lname"));
                System.out.println(new Gson().toJson(friends.next(), Vertex.class));
//                GraphSONUtil.writeWithType();
//                printValues(friends.next());
            }

            return Response.ok("OK").build();

        } catch (Exception e) {

            e.printStackTrace();
            return Response.serverError().build();
        }
    }

    /**
     * Get the database features information
     *
     * @return String containing database features
     */
    @GET
    @Path("/setting/about")
    @Produces(MediaType.TEXT_PLAIN)
    public Response aboutGraph() {

        Graph.Features features = graph.features();
        System.out.println(features.toString());
        return Response.ok(features.toString()).build();
    }

    /**
     * TODO:// this resource should give us status json
     *
     * @param name
     * @param limit
     * @return
     */
    @GET
    @Path("/stat")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stat(@PathParam("name") String name, @PathParam("limit") int limit) {

        System.out.println(IteratorUtils.count(TitanServer.graph.vertices()));
        System.out.println(IteratorUtils.count(TitanServer.graph.edges()));

        return Response.ok("{}").build();
    }

    /**
     * Saving graph to file with <b>JSON</b> or <i>XML</i> format
     *
     * @param format The output format which can be either JSON or XML
     * @return Returns a success server response (200) in case of success or
     * server error (500) in case of failure
     */
    @GET
    @Path("/setting/save/{format}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response saveGraph(@PathParam("format") String format) {

        String filepath;
        try {
            switch (format) {
                case "Kryo":
                case "kryo":
                case "KRYO":
                    filepath = "/tmp/graph-" + LocalDateTime.now() + "." + format;
                    saveGraphAsKryo(graph, filepath);
                    return Response.ok("graph Saved to " + filepath).build();
                case "Json":
                case "json":
                case "JSON":
                    filepath = "/tmp/graph-" + LocalDateTime.now() + "." + format;
                    saveGraphAsJson(graph, filepath);
                    //saveGraphAsJsonWithTypeInformation(graph, filepath);
                    return Response.ok("graph Saved to " + filepath).build();
                case "Xml":
                case "xml":
                case "XML":
                    filepath = "/tmp/graph-" + LocalDateTime.now() + "." + format;
                    saveGraphAsXml(graph, filepath);
                    return Response.ok("graph Saved to " + filepath).build();
                default:
                    return Response.serverError().entity("Wrong format").build();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return Response.serverError().entity("Failed to save the graph").build();
        }
    }

    private void saveGraphAsKryo(TitanGraph graph, String filepath) throws IOException {
        graph.io(IoCore.gryo()).writeGraph(filepath);
    }

    /**
     * Save graph with JSON format to file
     *
     * @param graph
     * @param filepath
     * @throws IOException
     */
    private void saveGraphAsJson(TitanGraph graph, String filepath) throws IOException {
        graph.io(IoCore.graphson()).writeGraph(filepath);
    }

    /**
     * Save graph as JSON with types
     *
     * @param graph
     * @param filepath
     * @throws IOException
     */
    private void saveGraphAsJsonWithTypeInformation(TitanGraph graph, String filepath) throws IOException {
        File f = new File(filepath);
        FileOutputStream fileOutputStream = new FileOutputStream(f);
        GraphTraversalSource g = TitanServer.graph.traversal();
        GraphSONMapper mapper = graph.io(IoCore.graphson()).mapper().version(GraphSONVersion.V1_0).embedTypes(true).create();
        graph.io(IoCore.graphson()).writer().mapper(mapper).create().writeVertex(fileOutputStream, g.V(1).next(), BOTH);
    }

    /**
     * Save graph as XML with GraphXML format
     *
     * @param graph
     * @param filepath
     * @throws IOException
     */
    private void saveGraphAsXml(Graph graph, String filepath) throws IOException {
        graph.io(IoCore.graphml()).writeGraph(filepath);
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(/*Collections.reverseOrder()*/))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * TODO: fix this. <br> Another similarity query but with gremlin query
     * instead of hybrid approach
     *
     * @param name
     * @param limit
     * @return
     */
    @GET
    @Path("/similar2/{name}/{limit}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response similarStudent2(@PathParam("name") String name, @PathParam("limit") int limit) {

        //find our student average
        ArrayList<Double> avgList = (ArrayList<Double>) g.V().hasLabel("student").has("name", name).valueMap("avg").next().get("avg");
        double avg = avgList.get(0);
        System.out.println("our avg =" + avg);

        //http://stackoverflow.com/questions/14486386/closest-to-average-value-sql
        //https://groups.google.com/forum/#!topic/gremlin-users/wgvHXSi6za0
        GraphTraversal<Vertex, Map<String, Object>> valueMap = g.V().has("name", name).out("friend").order().by(new Comparator<Vertex>() {
            @Override
            public int compare(Vertex o1, Vertex o2) {
                double v1 = o1.value("avg");
                double v2 = o2.value("avg");

                return (int) Math.abs(v2 - avg);
            }
        }).limit(limit).valueMap();

        ArrayList<Student> result = new ArrayList<>();
        while (valueMap.hasNext()) {
            Map<String, Object> next = valueMap.next();

            Student student = new Student();
            ArrayList<Double> al = (ArrayList<Double>) next.get("avg");
            student.setAvg(al.get(0));

            ArrayList<Integer> age = (ArrayList<Integer>) next.get("age");
            student.setAge(age.get(0));

            ArrayList<String> nameArrayList = (ArrayList<String>) next.get("name");
            student.setName(nameArrayList.get(0));

            ArrayList<String> lnameArrayList = (ArrayList<String>) next.get("lname");
            student.setLname(lnameArrayList.get(0));

            ArrayList<String> studyCity = (ArrayList<String>) next.get("study_city");
            student.setStudyCity(studyCity.get(0));

            ArrayList<String> livingCity = (ArrayList<String>) next.get("living_city");
            student.setLivingCity(livingCity.get(0));

            ArrayList<String> bornCity = (ArrayList<String>) next.get("born_city");
            student.setBornCity(bornCity.get(0));

            result.add(student);
        }

//        Comparator<Pair<Double, Map<String, Object>>> comparator = new Comparator<Pair<Double, Map<String, Object>>>() {
//            @Override
//            public int compare(Pair<Double, Map<String, Object>> o1, Pair<Double, Map<String, Object>> o2) {
//                return (int) (o1.left - o2.left);
//            }
//        };
//
//
//        double avg = 13.1;
//        //TODO:
//        GraphTraversal<Vertex, Vertex> g3 = g.V().has(T.label, "student").order().by("avg",
//                (Double x, Double y) -> {
//                    return (int) Math.abs(x - avg);
//                });
//
//        while (g3.hasNext()) {
//            Vertex next = g3.next();
//            System.out.println("-=-=-" + next.value("name") + " " + next.value("lname") + " " + next.value("avg"));
//        }
//
//        GraphTraversal<Vertex, Vertex> friends = g.V().hasLabel("student").order().by("avg", decr).limit(limit);
//
//        //order vertices by count of their incoming edges
//        GraphTraversal<Vertex, Vertex> g2 = g.V().hasLabel("student").order().by("avg", (Long x, Long y) -> {
//            return (x < y) ? -1 : ((x == y) ? 0 : 1);
//        });
        return Response.ok(result).build();
    }
}
