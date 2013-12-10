package data.jcascalog.classes;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import data.pail.topicpail.App;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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

        private String clean( Status tweet ){
           //remove emoticons
            String tmpString = tweet.getText().
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
            if( tweet.isRetweet() ){
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
            Path f = null;
            
            try {
              HadoopFlowProcess hfp = (HadoopFlowProcess) process;
              Path[] files = DistributedCache
                      .getLocalCacheFiles( hfp.getJobConf() );
              f = files[0];
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            try 
            {
                FileSystem fs = FileSystem.getLocal(new Configuration());
                InputStream in = fs.open(f);
                InputStreamReader inr = new InputStreamReader(in);
                try (BufferedReader r = new BufferedReader(inr))
                {
                    String line;
                    
                    while ((line = r.readLine()) != null)
                        stopList.add(line);
                }
            }
            catch( IOException e ) 
            {
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void operate( FlowProcess process, FunctionCall call ) {
            
            try
            {
                Status tweet = DataObjectFactory.createStatus(
                        call.getArguments().getString(0) );
                
                if( tweet.getIsoLanguageCode().equals("en") ){
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
                            
                            call.getOutputCollector().add( new Tuple( token ) );
                        }
                    }
                }
            } 
            catch( TwitterException | IOException e ) 
            {
                Logger.getLogger(App.class.getName())
                        .log(Level.SEVERE, null, e);
            }            
         }
    }
