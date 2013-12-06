package jcascalog.test;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tap.local.StdOutTap;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import com.twitter.maple.tap.StdoutTap;
import data.pail.topicpail.App;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import jcascalog.Api;
import jcascalog.Subquery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author nikos
 */
public class JCascTestMain extends Configured implements Tool {

    private static class CretateTokens extends CascalogFunction {

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
        public void operate( FlowProcess proccess, FunctionCall call ) {
            
            try
            {
                Status tweet = DataObjectFactory.createStatus(
                        call.getArguments().getString(0) );
                
                if( tweet.getIsoLanguageCode().equals("en") ){
                    String cleanTweet = clean( tweet );
                    TokenStream tokenizer = new StandardTokenizer(
                        Version.LUCENE_46, new StringReader( cleanTweet ) );
                    
                    try ( ShingleFilter filter2 = 
                            new ShingleFilter( tokenizer, 2, 5 ) )
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
                Logger.getLogger(JCascTestMain.class.getName())
                        .log(Level.SEVERE, null, e);
            }            
         }
    }
    
    @Override
    public int run( String[] args ) throws Exception 
    {
        Configuration hadConf = this.getConf();
        Map apiConf = new HashMap();
        String sers = "backtype.hadoop.ThriftSerialization," +
        "org.apache.hadoop.io.serializer.WritableSerialization";
        apiConf.put("io.serializations", sers);
        
        Iterator<Map.Entry<String, String>> iter = hadConf.iterator();
        while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                apiConf.put(entry.getKey(), entry.getValue());
        }
        
        Api.setApplicationConf(apiConf);
        
        Subquery tokens = new Subquery( "?token" )
                .predicate( Api.hfsTextline( "/tmp/test1000.txt" ), "?tweets" )
                .predicate( new CretateTokens(), "?tweets" ).out( "?token" );
        
        Api.execute( new StdoutTap(), tokens );
        
        return 0;
    }
    
    /**
     * @param args
     * 
     */
    public static void main( String[] args ) 
    {
        Configuration conf = new Configuration();
        conf.set( "mapred.child.java.opts", "-Xmx2g" );
        try {
            ToolRunner.run(conf, new JCascTestMain(), args);
        } catch (Exception ex) {
            Logger.getLogger(JCascTestMain.class.getName())
                    .log(Level.SEVERE, null, ex);
        }
    }
    
}
