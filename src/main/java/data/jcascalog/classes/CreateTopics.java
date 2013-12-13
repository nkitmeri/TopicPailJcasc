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
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class CreateTopics extends CascalogFunction {
        
        private List< String > stopList;
        private List< String > trendsNotFulfilRequirements;
        private Map< String, Long > trendsList;

        private String clean( Tweet tweet ){
           //remove emoticons
            String tmpString = tweet.getText().orignalText.
                    replaceAll("\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~"
                            + "_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", "").
                    replaceAll("[^A-Za-z@#0-9-\\s+]", "");
            //tokenize tweet
            String [] tweetWords = tmpString.split("\\s+");
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
            
            Path f;
            
            try {
                HadoopFlowProcess hfp = (HadoopFlowProcess) process;
                Path[] files = DistributedCache
                        .getLocalCacheFiles( hfp.getJobConf() );
                FileSystem fs = FileSystem.getLocal(new Configuration());
                InputStream in;
                InputStreamReader inr;
                

                //read StopWords List
                f = files[0];
                in = fs.open(f);
                inr = new InputStreamReader(in);
                try (BufferedReader r = new BufferedReader(inr))
                {
                    String line;

                    while ((line = r.readLine()) != null)
                        stopList.add(line);
                }
                 //read Trends Map
                f = files[1];
                in = fs.open(f);
                inr = new InputStreamReader(in);
                DateFormat df =
                        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                try (BufferedReader r = new BufferedReader(inr))
                {
                    String line;

                    while ((line = r.readLine()) != null)
                        trendsList.put(line.split(",")[0],df.
                                parse( line.split(",")[1] ).getTime());
                }
                
                //read Trends Not Fulfil Requirements List
                f = files[2];
                in = fs.open(f);
                inr = new InputStreamReader(in);
                try (BufferedReader r = new BufferedReader(inr))
                {
                    String line;

                    while ((line = r.readLine()) != null)
                        trendsNotFulfilRequirements.add(line);
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
                Tweet tweet = ( Tweet ) call.getArguments().getObject(0);
                
                String cleanTweet = clean( tweet );
                TokenStream tokenizer = new StandardTokenizer(
                    Version.LUCENE_46, new StringReader( cleanTweet ) );
                StopFilter filter = new StopFilter( Version.LUCENE_46, 
                        tokenizer, StopFilter.makeStopSet( 
                                Version.LUCENE_46, stopList, true ) );

                try ( ShingleFilter filter2 = 
                        new ShingleFilter( filter, 2, 5 ) )
                {
                    CharTermAttribute termAtt = filter2
                            .addAttribute( CharTermAttribute.class );
                    tokenizer.reset();

                    String token;

                    while( filter2.incrementToken() )
                    {
                        token = termAtt.toString();

                        if ( token.contains("_") )
                        {
                            continue;
                        }
                        if ( trendsList.containsKey(token) )
                        {
                            call.getOutputCollector().add( new Tuple( token,
                                    true, trendsList.get(token),
                                    tweet.date.getCratedAt().toString() ) );
                        }
                        else if ( !trendsNotFulfilRequirements.contains(token) )
                        {
                            Tuple tuple = new Tuple( token, false, null,
                                    tweet.date.getCratedAt().toString() );
                            call.getOutputCollector().add( tuple );
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
