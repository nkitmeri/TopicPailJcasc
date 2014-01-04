
package data.jcascalog.queries;

import com.backtype.cascading.tap.PailTap;
import com.backtype.cascading.tap.PailTap.PailTapOptions;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import data.jcascalog.classes.CreateBucketsBuf;
import data.jcascalog.classes.CreateTopics;
import data.pail.tweetpail.SplitTweetPailStructure;
import jcascalog.Subquery;

/**
 *
 * @author nikos
 */
public class Queries 
{
    private final String args;
    private final String subquery;
    
    public Queries( String args, String subquery )
    {
        this.args = args;
        this.subquery = subquery;
    }
    
    public Subquery getQuery()
    {
        switch( subquery )
        {
            case "?cleanTokens, ?isTrend, !timeTrended, ?timeBuckets":
                return new Subquery( subquery.split( ", ") )
                .predicate( splitTweetTap( args ), "_", "?tweets" )
                .predicate( new CreateTopics(), "?tweets" )
                .out( "?cleanTokens", "?isTrend", "!timeTrended"
                        , "?tweetTime" )
                .predicate( new CreateBucketsBuf(), "?tweetTime" )
                        .out( "?timeBuckets" );
               
            case "?cleanTokens, ?isTrend, !timeTrended, ?tweetTime":
                return new Subquery( subquery.split( ", ") )
                .predicate( splitTweetTap( args ), "_", "?tweets" )
                .predicate( new CreateTopics(), "?tweets" )
                .out( "?cleanTokens", "?isTrend", "!timeTrended"
                        , "?tweetTime" );
                
            default:
                System.err.println( "Not valid subquery" );
                break;
        }
        
        throw new RuntimeException();
    }
    
    public static PailTap splitTweetTap(String path) {
        PailTapOptions opts = new PailTapOptions();
        opts.spec = new PailSpec((PailStructure) new SplitTweetPailStructure());
        return new PailTap(path, opts);
    }
}
