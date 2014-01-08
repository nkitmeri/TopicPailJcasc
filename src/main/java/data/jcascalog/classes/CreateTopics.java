package data.jcascalog.classes;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import data.thrift.tweetthrift.Tweet;
import data.thrift.tweetthrift.TweetType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class CreateTopics extends CascalogFunction {
        
        private static List< String > stopList;
        private static List< String > trendsNotFulfilRequirements;
        private static Map< String, Long > trendsList;
        private static String tweetDate = "";
        private static final Tuple tuple = Tuple.size(4);
        private static final Random rand = new Random();
        private static final int RAND_LIMIT = 100000;
        private static final int RAND_PASS = 50000;
        private static String tmpString;
        private static String [] tweetWords;
        private static Path f;
        private static HadoopFlowProcess hfp;
        private static Path[] files;
        private static FileSystem fs;
        private static InputStream in;
        private static InputStreamReader inr;
        private static String line;
        private static DateFormat df;
        private static Tweet tweet;
        private static String cleanTweet;
        private static TokenStream tokenizer;
        private static StopFilter filter;
        private static CharTermAttribute termAtt;
        private static String token;

        private String clean( Tweet tweet ){
           //remove emoticons
            tmpString = tweet.getText().orignalText.
                    replaceAll("\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~"
                            + "_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", "").
                    replaceAll("[^\\p{L}\\p{N}\\s]+", "");
                    //replaceAll("[^A-Za-z@#0-9-\\s+]", "");
            //tokenize tweet
            tweetWords = tmpString.split("\\s+");
            for( int i=0; i< tweetWords.length; i++ ){
                 //remove mentions 
                 if ( tweetWords[i].length() >= 1 &&
                         ( tweetWords[i].contains("@") )  )
                 {
                     tweetWords[i] = "";
                 }

            }
            //remove retweets RT
            if( tweet.getType().equals( TweetType.RETWEET ) ){
                tweetWords[0] = "";
            }
            tmpString = "";
            for( String y : tweetWords ){
                tmpString += y+" "; 
            }        

            return tmpString.trim();
        }
        
        @Override
        public void prepare( FlowProcess process, OperationCall call )
        {
            super.prepare( process, call );
            stopList = new ArrayList<>();
            trendsList = new HashMap<>();
            trendsNotFulfilRequirements = new ArrayList<>();
            
            try {
                hfp = (HadoopFlowProcess) process;
                files = DistributedCache
                        .getLocalCacheFiles( hfp.getJobConf() );
                fs = FileSystem.getLocal(new Configuration());    

                //read StopWords List
                f = files[0];
                in = fs.open(f);
                inr = new InputStreamReader(in);
                try (BufferedReader r = new BufferedReader(inr))
                {
                    while ((line = r.readLine()) != null)
                        stopList.add(line);
                }
                 //read Trends Map
                f = files[1];
                in = fs.open(f);
                inr = new InputStreamReader(in);
                df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                
                try (BufferedReader r = new BufferedReader(inr))
                {
                    while ((line = r.readLine()) != null)
                    {
                        if( line.charAt(0) == '#' )
                            line = line.substring(1);
                        trendsList.put(line.split(",")[0],df.
                                parse( line.split(",")[1] ).getTime());
                    }
                }
                
                //read Trends Not Fulfil Requirements List
                f = files[2];
                in = fs.open(f);
                inr = new InputStreamReader(in);
                try (BufferedReader r = new BufferedReader(inr))
                {
                    while ((line = r.readLine()) != null) { 
                        if( line.charAt(0) == '#' )
                            line = line.substring(1);
                        trendsNotFulfilRequirements.add(line);
                    }
                } 
            }
            catch( IOException | ParseException e ) 
            {
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void operate( FlowProcess process, FunctionCall call ) {
            
            try
            {
                tweet = ( Tweet ) call.getArguments().getObject(0);
                tweetDate = tweet.date.getCratedAt().toString();
                cleanTweet = clean( tweet );
                tokenizer = new StandardTokenizer(
                    Version.LUCENE_46, new StringReader( cleanTweet ) );
                filter = new StopFilter( Version.LUCENE_46, 
                        tokenizer, StopFilter.makeStopSet( 
                                Version.LUCENE_46, stopList, true ) );
                
                try ( ShingleFilter filter2 = 
                        new ShingleFilter( filter, 2, 5 ) )
                {
                    termAtt = filter2.addAttribute( CharTermAttribute.class );
                    tokenizer.reset();            

                    while( filter2.incrementToken() )
                    {
                        token = termAtt.toString();
                        
                        if ( token.contains("_") )
                        {
                            continue;
                        }
                        
                        if ( trendsList.containsKey(token) )
                        {
                            tuple.set( 0, token );
                            tuple.set( 1, true );
                            tuple.set( 2, trendsList.get(token) );
                            tuple.set( 3, tweetDate );
                            
                            call.getOutputCollector().add( tuple ); 
                        }
                        else if ( rand.nextInt( RAND_LIMIT ) == RAND_PASS )
                        {
                            if( !trendsNotFulfilRequirements.contains(token) )
                            {
                                tuple.set( 0, token );
                                tuple.set( 1, false );
                                tuple.set( 2, null );
                                tuple.set( 3, tweetDate );
                                                       
                                call.getOutputCollector().add( tuple );
                            }
                        }
                    }
                }
            }             
            catch( IOException e ) 
            {
                throw new RuntimeException(e);
            }            
         }
    }
