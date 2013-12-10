
package data.jcascalog.queries;

import data.jcascalog.classes.CreateTopics;
import jcascalog.Api;
import jcascalog.Option;
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
            case "?cleanTokens, ?isTrend, !timeTrended":
                return new Subquery( subquery.split( ", ") )
                .predicate( Api.hfsTextline( args ), "?tweets" )
                .predicate( new CreateTopics(), "?tweets" )
                .out( "?cleanTokens", "?isTrend", "!timeTrended" )
                        .predicate( Option.DISTINCT, true );
                
            default:
                System.err.println( "Not valid subquery" );
                break;
        }
        
        throw new RuntimeException();
    }
}
