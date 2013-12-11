package data.pail.tweetpail;

import data.pail.app.ThriftPailStructure;
import data.thrift.tweetthrift.Tweet;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author nikos
 */
public class TweetPailStructure extends ThriftPailStructure<Tweet>
{
    @Override
    public Class getType() {
        return Tweet.class;
    }
    
    @Override
    protected Tweet createThriftObject() {
        return new Tweet();
    }
    
    @Override
    public List<String> getTarget(Tweet t) {
        return Collections.EMPTY_LIST;
    }
    
    @Override
    public boolean isValidTarget(String... strings) {
        return true;
    }
    
}
