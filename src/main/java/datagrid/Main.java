package datagrid;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import akka.pattern.Patterns;
import akka.routing.RoundRobinPool;
import akka.util.Timeout;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static akka.dispatch.Futures.sequence;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by jkolb on 5/19/2014.
 */
public class Main
{

    long bytesToWriteInLargeFile = 1048576L * 1024 * 10; // 10 GB
    int maximumChunkSizeInBytes = 1048576*5; // 5 MB
    long maxRequests = 600; // 1048576L * 1024 * 5 / maximumChunkSizeInBytes; // 5gigs worth of requests
    String parentTypeName = "parenttype";
    String childTypeName = "childtype";

    @Option(name="-operation",usage="Operation to run ('partitionTest', 'sizeTest')")
    static String operation = "partitionTest";

    @Option(name="-docSizeToTest",usage="Document size to test (in bytes)")
    static Integer bytesToTest = 104857000; // 100 MB - 600 bytes for request stuff

    @Option(name="-endpoint",usage="ES endpoint to connect to")
    static String endpoint = "http://localhost:9200";

    @Option(name="-index",usage="index to write to")
    String indexName = "foo"; // "audit__edds1036563_audit";

    @Option(name="-type",usage="the elasticsearch type to use")
    String typeName = "audit";

    @Option(name="-data",usage="the data path to read the sample data from")
    String dataPath = "c:\\data\\audit.txt";

    @Option(name="-number",usage="the number of writes to perform")
    Integer number = 10;

    @Option(name="-sleepmsecs",usage="the length of time to pause in between write transactions")
    Integer sleepmsecs = 1000;

    // receives other command line parameters than options
    @Argument
    List<String> arguments = new ArrayList<String>();

    public static void main( String[] args ) throws Exception {
        new Main().doMain(args);
    }

    public void doMain(String[] args) throws Exception {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            // parse the arguments.
            parser.parseArgument(args);

            // you can parse additional arguments if you want.
            // parser.parseArgument("more","args");

            // after parsing arguments, you should check
            // if enough arguments are given.
//            if( arguments.isEmpty() )
//                throw new CmdLineException(parser,"No argument is given");

        } catch( CmdLineException e ) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java -classpath datagrid.jar datagrid.Main [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            return;
        }

        // OnehundredMillionChildrenTest();
        // largeFileTest();
        // writeAudits();

        if( operation.equals("partitionTest") )
            doParititionTest();
        else if( operation.equals("sizeTest") )
            documentSizeTest();

    }

    public void doParititionTest() throws Exception {

        String payload = null;
        Path newPath = Paths.get(dataPath);
        try {
            byte[] encoded = Files.readAllBytes(newPath);
            payload = new String(encoded, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(endpoint)
                .multiThreaded(true)
                .build());
        final JestClient client = factory.getObject();

        Map<Integer,String> writtenIds = new HashMap<Integer,String>();
        Map<Integer,String> failedIds = new HashMap<Integer,String>();

        for( int i = 0; i < number; i++ ){


            try {
                Index index = new Index.Builder(payload).index(indexName).type(typeName).id(new Integer(i).toString()).build();
                JestResult x = client.execute(index);
                System.out.println("Indexing document #" + i + " then sleeping for " + this.sleepmsecs + " msecs");
                if( x.isSucceeded() )
                    writtenIds.put(i, "x");
                else {
                    System.out.println("Failed to index document " + i);
                    failedIds.put(i, "x");
                }
            } catch( Exception ex ){
                System.out.println("Failed to index document " + i);

                failedIds.put(i, "x");
            }


            Thread.sleep(this.sleepmsecs);
        }

        System.out.println("Press enter to start verifying");
        System.in.read();

        Integer total = writtenIds.size();
        Integer failedWriteVerifications = 0;

        // Verify written
        for( Integer thisId : writtenIds.keySet() ){
            String idToCheck = thisId.toString();

            Get get = new Get.Builder(indexName, idToCheck).type(typeName).build();
            JestResult result = client.execute(get);

            if( result.isSucceeded() ) {
                failedWriteVerifications++;
                System.out.println("Successful write ID " + idToCheck + " verified");
            } else {
                System.out.println("Successful write ID " + idToCheck + " MISSING!!!!!!!!!!!!!!!!!!!!!");
            }
        }

        Integer failedTotal = failedIds.size();

        Integer failedFailureVerifications = 0;

        // Verify failed
        for( Integer thisId : failedIds.keySet() ){
            String idToCheck = thisId.toString();

            Get get = new Get.Builder(indexName, idToCheck).type(typeName).build();
            JestResult result = client.execute(get);

            if( result.isSucceeded() ) {
                failedFailureVerifications++;
                System.out.println("Previously failed write ID " + idToCheck + " was verified (?)");
            } else {
                System.out.println("Previously failed write ID " + idToCheck + " missing (as expected)");
            }
        }

        Integer verified = total - failedWriteVerifications;
        Double verifiedPercent = verified.doubleValue() / total.doubleValue() * 100;
        System.out.println("Verified " + verified + " out of " + total + " reported as successful failed: (" + verifiedPercent + "%)");

        verified = failedTotal - failedFailureVerifications;
        verifiedPercent = verified.doubleValue() / failedTotal.doubleValue() * 100;
        System.out.println("Verified " + verified + " out of " + failedTotal + " reported as failures failed: (" + verifiedPercent + "%)");

    }

    public void documentSizeTest() throws Exception {

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(endpoint)
                .connTimeout(600000)
                .readTimeout(600000)
                .multiThreaded(true)
                .build());
        final JestClient client = factory.getObject();

        for( int i = 0; i < number; i++ ){

            System.out.println("Indexing document #" + i + " then sleeping for " + this.sleepmsecs + " msecs");

            String doc = generateDocument();

            String payload = jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", "date")
                    .field("payload", doc)
                    .endObject().string();

            try {
                Index index = new Index.Builder(payload).index(indexName).type(typeName).id(new Integer(i).toString()).build();
                JestResult x = client.execute(index);
                if( x.isSucceeded() )
                    System.out.println("Document " + i + " indexed successfully");
                else {
                    System.out.println("Document " + i + " FAILED");
                }
            } catch( Exception ex ){
                System.out.println("Document " + i + " FAILED");
            }

            Thread.sleep(this.sleepmsecs);
        }


        System.out.println("Done with size test");

    }

    public static String generateDocument()
    {
        StringBuilder result = new StringBuilder();

        Random random = new Random();
        while( result.length() < bytesToTest ){
            // Create a word
            char[] word = new char[random.nextInt(8)+3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for(int j = 0; j < word.length; j++)
            {
                word[j] = (char)('a' + random.nextInt(26));
            }
            result.append(new String(word) + " ");
        }

        return result.toString();
    }

    public void OnehundredMillionChildrenTest() throws Exception {
        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(endpoint)
                .multiThreaded(true)
                .build());
        final JestClient client = factory.getObject();

        // Create the index\
        client.execute(new CreateIndex.Builder(indexName).build());

        // Create the parent/child mappings
        // You can create an index mapping via Jest with ease; you just need to pass the mapping source JSON document as string.

        // creating the index mappings via manual PUT request for now. didn't want to waste time figuring out this call

        // Read the file in 100MB chunks

        // Create th eparent
        Map<String,String> source1 = new LinkedHashMap<String,String>();
        source1.put("docid", "document1");

        byte[] buf = new byte[maximumChunkSizeInBytes];
        int i = 0;

        String parentid = "only_parent";

        while (i < 100000000) {

            final int x = i;


                    final long indexNumber = x;

                    XContentBuilder jsonBuilder = null;
                    try {
                        jsonBuilder = jsonBuilder()
                                .startObject()
                                .field("user", "kimchy")
                                .field("postDate", "date")
                                .field("chunkIndex", x)
                     .field("parenttype", parentid);

//            jsonBuilder.startObject("_parent").field("type", parentTypeName).endObject();

                        jsonBuilder.endObject();

                    String source2 = null;
                        source2 = jsonBuilder.string();

                    // System.out.println("Submitted chunk " + indexNumber);

                    Index index = new Index.Builder(source2).index(indexName).type(childTypeName).setParameter("parent", parentid).build();
                        client.execute(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }


            if( i % 1000 == 0 )
                System.out.println("Indexing child #" + i);

            i++;
        }
    }

    public void parentChildTest() throws Exception {

        // Construct a new Jest client according to configuration via factory
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(endpoint)
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();

        // Create the index\
        client.execute(new CreateIndex.Builder(indexName).build());

        // Create the parent/child mappings
        // You can create an index mapping via Jest with ease; you just need to pass the mapping source JSON document as string.

        // creating the index mappings via manual PUT request for now. didn't want to waste time figuring out this call

        // Read the file in 100MB chunks
        Path newPath = Paths.get("c:\\data\\out.txt");

        // Create th eparent
        Map<String, String> source1 = new LinkedHashMap<String, String>();
        source1.put("docid", "document1");

        String parentid = "parent1";

        Index index1 = new Index.Builder(source1).index(indexName).type(parentTypeName).id(parentid).build();
        client.execute(index1);

        long bytesWritten = 0L;

        System.out.println("Started at " + new Date());

        InputStream is = new FileInputStream(newPath.toFile());

        byte[] buf = new byte[maximumChunkSizeInBytes];
        int bytesRead = is.read(buf);

        System.out.println("Max reqests: " + maxRequests);

        final AtomicInteger runningRequests = new AtomicInteger();

        int i = 0;
        while (bytesWritten < bytesToWriteInLargeFile) {

            final long finalBytesWritten = bytesWritten;
            final int indexNumber = i;
            String output = new String(buf, "US-ASCII");

            XContentBuilder jsonBuilder = jsonBuilder()
                    .startObject()
                    .field("user", "kimchy")
                    .field("postDate", "date")
                    .field("message", output)
                    .field("chunkIndex", i);
            // .field("parenttype", parentid);

//            jsonBuilder.startObject("_parent").field("type", parentTypeName).endObject();

            jsonBuilder.endObject();

            String source2 = jsonBuilder.string();

            Boolean waiting = false;
            if( runningRequests.get() > maxRequests ) {
                System.out.println("Taking a breather...");
                waiting = true;
            }

            while( runningRequests.get() > maxRequests )
                ; // wait

            if( waiting )
                System.out.println("Resuming...");

            Index index = new Index.Builder(source2).index(indexName).type(childTypeName).id("child" + i).setParameter("parent", parentid).build();
            JestResultHandler<JestResult> handler = new JestResultHandler<JestResult>() {
                @Override
                public void completed(JestResult result) {
                    // System.out.println("Successfully processed chunk " + indexNumber + " (" + (finalBytesWritten / 1048576) + "MB)");
                    runningRequests.decrementAndGet();
                }

                @Override
                public void failed(Exception ex) {
                    System.out.println("FAILED to process chunk " + indexNumber);
                    runningRequests.decrementAndGet();
                }
            };
            client.executeAsync(index, handler);
//            client.execute(index);

            bytesWritten += bytesRead;

            if( bytesWritten >= bytesToWriteInLargeFile )
                break;

            runningRequests.incrementAndGet();

            i++;
            System.out.println("So far, have written " + (bytesWritten / 1048576L) + " MB - open requests: " + runningRequests.get());
        }

        while( runningRequests.get() > 0 )
            ; // wait

        System.out.println("Wrote " + bytesWritten + " bytes");
        System.out.println("Ended at " + new Date());
    }

    static TransportClient client;
    static String audit;
    static int numberActors = 200;

    public class AuditTest extends UntypedActor
    {
        int i = 0;

        public AuditTest( String foo ){
        }

        public void onReceive( Object message ) throws Exception{

            while (i < 1000000) {

                IndexResponse response = client.prepareIndex(indexName, typeName, UUID.randomUUID().toString())
                        .setSource(audit
                        )
                        .execute()
                        .actionGet();

                if (i % 1000 == 0)
                    System.out.println("Indexing child #" + i + ":" + response.isCreated());

                i++;
            }
        }
    }

    public class MyActorC implements Creator<AuditTest> {
        @Override public AuditTest create() {
            return new AuditTest("...");
        }
    }

    public void init(){

        Path newPath = Paths.get(dataPath);
        try {
            byte[] encoded = Files.readAllBytes(newPath);
            audit = new String(encoded, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Settings s = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "PF4ES")
                .build();
        client = new TransportClient(s);
        client.addTransportAddress(new InetSocketTransportAddress(
                        "172.16.38.122",
                        // "localhost",
                        9300)
        );

    }

    public void writeAudits() throws Exception {


        init();

        IndexResponse response = client.prepareIndex(indexName, typeName, UUID.randomUUID().toString())
                .setSource(audit
                )
                .execute()
                .actionGet();

        ActorSystem system = ActorSystem.create("PiSystem");
        final ActorRef listener= system.actorOf(new RoundRobinPool(numberActors).props(Props.create(new MyActorC())));

        int i = 0;

//        JestResultHandler<JestResult> handler = new JestResultHandler<JestResult>() {
//            @Override
//            public void completed(JestResult result) {
//                // System.out.println("Successfully processed chunk " + indexNumber + " (" + (finalBytesWritten / 1048576) + "MB)");
//            }
//
//            @Override
//            public void failed(Exception ex) {
//                System.out.println("FAILED to process audit");
//            }
//        };


        ArrayList<Future<Object>> results = new ArrayList<Future<Object>>();

        while (i < numberActors) {

            final int x = i;

            Timeout timeout = new Timeout(Duration.create(1, "hour"));
            results.add(Patterns.ask(listener, "write", timeout));

            i++;
        }

        Timeout timeout = new Timeout(Duration.create(1, "hour"));
        Future<Iterable<Object>> futureListOfInts = sequence(results, system.dispatcher());

        System.out.println("Main thread waiting");
        Await.result(futureListOfInts, timeout.duration());
        System.out.println("Main thread DONE");
    }
}
