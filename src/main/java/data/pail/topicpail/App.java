package data.pail.topicpail;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.twitter.maple.tap.StdoutTap;
import data.thrift.topicthrift.Topic;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import jcascalog.Api;
import jcascalog.Subquery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    
    public static class TopicJSON{
        private String name;
        private boolean isTrend;
        private long timeTrended;
        private ArrayList<Long> timeStamps;
        private ArrayList<Long> tweetID;

        public String getName(){
            return name;
        }

        public boolean getIsTrend(){
            return isTrend;
        }

        public long getTimeTrended(){
            return timeTrended;
        }

        public ArrayList<Long> getTimeStamps(){
            return timeStamps;
        }

        public ArrayList<Long> getTweetID(){
            return tweetID;
        }

        public void setName( String name){
            this.name = name;
        }

        public void setIsTrend( Boolean isTrend){
            this.isTrend = isTrend;
        }

        public void setTimeTrended( long timeTrended ){
            this.timeTrended = timeTrended;
        }

        public void setTimeStamps(  ArrayList<Long> timeStamps ){
            this.timeStamps = timeStamps; 
        }

        public void setTweetID(  ArrayList<Long> tweetID ){
            this.tweetID = tweetID; 
        }
    }

    private static Map<String, Object> options;
    private static Pail compressed;
    private static Pail.TypedRecordOutputStream os;
    private static String firstDateStr;
    
    public static void createCompressedPail() throws IOException {
        options = new HashMap<>();
        options.put(SequenceFileFormat.CODEC_ARG, 
                SequenceFileFormat.CODEC_ARG_BZIP2);
        options.put(SequenceFileFormat.TYPE_ARG,
                SequenceFileFormat.TYPE_ARG_BLOCK);
        SplitTopicPailStructure struct = new SplitTopicPailStructure();
        compressed = Pail.create("/user/user/tmp/compressed",
                new PailSpec("SequenceFile", options, struct));
        os = compressed.openWrite();
    }
    
//    public static void setApplicationConf() {
//        Map conf = new HashMap();
//        String sers = /*"backtype.hadoop.ThriftSerialization," +*/
//        "org.apache.hadoop.io.serializer.WritableSerialization";
//        conf.put("io.serializations", sers);
//        Api.setApplicationConf(conf);
//    }

    public static class CreateTopic extends CascalogFunction {

        @Override
        public void operate(FlowProcess process, FunctionCall call) {
            String json = call.getArguments().getString(0);
            Gson gson = new Gson();
            
            TopicJSON topicJSON = gson.fromJson( json , TopicJSON.class );
            DateFormat df = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
            Date firstDate = null;//,lastDate;
            try { 
                firstDate = df.parse( firstDateStr );
            } catch (ParseException ex) {
                Logger.getLogger(App.class.getName())
                        .log(Level.SEVERE, null, ex);
            }
//            lastDate = df.parse( args[1] ); 
            int poss;

            Map<Integer,Double> tmptimeSeries;
            Topic topic;
            try {
                topic = new Topic();
                topic.setName( topicJSON.getName() );
                topic.setIsTrend( topicJSON.getIsTrend() );
                topic.setTrendDate( topicJSON.getTimeTrended());
                tmptimeSeries = new HashMap<>();
                for(int j=0; j<topicJSON.getTimeStamps().size(); j++){
                    poss =(int) ((( topicJSON.getTimeStamps().get(j)
                                   - firstDate.getTime() ) / ( 60 * 1000 )) / 2);
                    if( tmptimeSeries.containsKey(poss) ){
                        tmptimeSeries.put(poss, tmptimeSeries.get(poss) + 1.0);
                    }
                    else{
                        tmptimeSeries.put(poss, 1.0);
                    }
                }
                topic.setTimeSeries(tmptimeSeries);
//                    thriftTopic.add(topic);
                call.getOutputCollector().add( new Tuple( topic ) );
            }
            catch(OutOfMemoryError e){
                e.printStackTrace(System.err);
            }
        }
        
    }
    
    @Override
    public int run( String[] args ) throws Exception {
        
        Configuration hadConf = this.getConf();
        Map apiConf = new HashMap();
        String sers = "backtype.hadoop.ThriftSerialization," +
        "org.apache.hadoop.io.serializer.WritableSerialization";
        apiConf.put("io.serializations", sers);
        
        Iterator<Entry<String, String>> iter = hadConf.iterator();
        while (iter.hasNext()) {
                Entry<String, String> entry = iter.next();
                apiConf.put(entry.getKey(), entry.getValue());
        }
        
        Api.setApplicationConf(apiConf);
        firstDateStr = args[0];
//        setApplicationConf();
//        Topic topic = new Topic();

        Api.execute(
            new StdoutTap(),
            new Subquery("?topic")
              .predicate(Api.hfsTextline(args[1]), "?input")
              .predicate(new CreateTopic(), "?input").out( "?topic" ) );
        
        
        return 0;
    }
    
    public static void main( String[] args ) throws IOException, ParseException, Exception
    {
//        Configuration conf = new Configuration();
//        Job job = new Job( conf );
//        job.setJarByClass( App.class );
//        createCompressedPail();
//        
//        // Local test for Pail.
//        Gson gson = new Gson();
//        
//        for ( File f : new File( "/user/user/tmp/topicJsonSample/" ).listFiles() ) { 
//            BufferedReader bufferedReader = new BufferedReader(new FileReader(
//                    f));
//            ArrayList< TopicJSON > topicJSON = gson.fromJson( bufferedReader , 
//                    new TypeToken<ArrayList<TopicJSON>>(){}.getType());
//            DateFormat df = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
//            Date firstDate,lastDate;
//            firstDate = df.parse( args[0] ); 
//            lastDate = df.parse( args[1] ); 
//            int poss;
//
//            Map<Integer,Double> tmptimeSeries;
//            Topic topic;
//            for(int i=0; i<topicJSON.size(); i++){
//                try {
//                    topic = new Topic();
//                    topic.setName( topicJSON.get(i).getName() );
//                    topic.setIsTrend( topicJSON.get(i).getIsTrend() );
//                    topic.setTrendDate( topicJSON.get(i).getTimeTrended());
//                    tmptimeSeries = new HashMap<>();
//                    for(int j=0; j<topicJSON.get(i).getTimeStamps().size(); j++){
//                        poss =(int) ((( topicJSON.get(i).getTimeStamps().get(j)
//                                       - firstDate.getTime() ) / ( 60 * 1000 )) / 2);
//                        if( tmptimeSeries.containsKey(poss) ){
//                            tmptimeSeries.put(poss, tmptimeSeries.get(poss) + 1.0);
//                        }
//                        else{
//                            tmptimeSeries.put(poss, 1.0);
//                        }
//                    }
//                    topic.setTimeSeries(tmptimeSeries);
////                    thriftTopic.add(topic);
//
//                    os.writeObject( topic );
//                }
//                catch(OutOfMemoryError e){
//                    e.printStackTrace(System.err);
//                }
//            }
//        }
//        
//        os.close();
        
        // JCascalog
        
//        firstDateStr = args[0];
//        setApplicationConf();
//        Topic topic = new Topic();
//
//        Api.execute(
//            new StdoutTap(),
//            new Subquery("?topic")
//              .predicate(Api.hfsTextline(args[1]), "_")
//              .predicate(new CreateTopic(), "?topic"));
        
        Configuration conf = new Configuration();
        conf.set( "mapred.child.java.opts", "-Xmx2g" );
        ToolRunner.run(conf, new App(), args);
        
    }
    
}
