package tmp;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailSpec;

import com.backtype.hadoop.pail.SequenceFileFormat;
import data.pail.tweetpail.SplitTweetPailStructure;
import data.thrift.tweetthrift.Text;
import data.thrift.tweetthrift.Tweet;
import data.thrift.tweetthrift.TweetType;
import data.thrift.tweetthrift.User;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author nikos
 */
public class NewMain {

    /**
     * @param args the command line arguments
     * @throws java.io.FileNotFoundException
     * @throws twitter4j.TwitterException
     */
    public static void main(String[] args) 
            throws FileNotFoundException, TwitterException, IOException
    {
//        File folder = new File( "/tmp/tweets" );
//        File[] files = folder.listFiles();
//        Map<String, Object> options = new HashMap<>();
//        options.put(SequenceFileFormat.CODEC_ARG,
//            SequenceFileFormat.CODEC_ARG_BZIP2);
//        options.put(SequenceFileFormat.TYPE_ARG,
//            SequenceFileFormat.TYPE_ARG_BLOCK);
//        SplitTweetPailStructure struct = new SplitTweetPailStructure();
//        Pail pail = Pail.create("/home/nikos/temp/compressed",
//            new PailSpec("SequenceFile", options, struct));
//        TypedRecordOutputStream os = pail.openWrite();
//        String line;
//        
//        for( File f : files )
//        {
//            FileInputStream is = new FileInputStream( f );
//            InputStreamReader inr = new InputStreamReader( is );
//
//            try( BufferedReader br = new BufferedReader( inr ) )
//            {
//                while( ( line = br.readLine() ) != null )
//                {
//                    try
//                    {
//                        Status tweet = DataObjectFactory.createStatus( line );
//
//                        Tweet tTweet = new Tweet( tweet.getId(),
//                                new User( tweet.getUser().getId() ),
//                                ( tweet.isRetweet() ? TweetType.RETWEET 
//                                        : TweetType.TWEET ),
//                                new data.thrift.tweetthrift.Date( 
//                                        tweet.getCreatedAt().toString() ) ); 
//
//                        tTweet.setText( new Text( tweet.getText() ) );
//
//
//                        os.writeObject(tTweet); 
//                    }
//                    catch( TwitterException e ) {}
//                }
//            }
//        }
//        os.close();
        
        Pail<Tweet> pail = new Pail( "/tmp/thriftTweets" );
        Pail<Tweet> pail2 = new Pail( "/tmp/thriftTweets2" );
        long cnt = 0;
        
        for( Tweet t : pail ) cnt++;
        for( Tweet t : pail2 ) cnt++;
        
        System.out.println( cnt );
            
    }
    
}
