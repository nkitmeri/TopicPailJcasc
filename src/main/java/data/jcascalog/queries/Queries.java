
package data.jcascalog.queries;

import data.jcascalog.classes.CreateTopics;
import jcascalog.Api;
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
            case "?cleanTokens, ?isTrend":
                return new Subquery( subquery.split( ", ")[0],
                        subquery.split( ", ")[1] )
                .predicate( Api.hfsTextline( args ), "?tweets" )
                .predicate( new CreateTopics(), "?tweets" )
                .out( "?cleanTokens" );
                
            default:
                System.err.println( "Not valid subquery" );
                break;
        }
        
        throw new RuntimeException();
    }
}
