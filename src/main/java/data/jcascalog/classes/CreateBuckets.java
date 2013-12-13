package data.jcascalog.classes;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.tuple.Tuple;
import cascalog.CascalogAggregator;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nikos
 */
public class CreateBuckets extends CascalogAggregator
{
    private static final DateFormat DATEFORMAT =
                    new SimpleDateFormat( "EEE MMM dd HH:mm:ss ZZZZZ yyyy" );
    private static long firstDate;
    
    static
    {
        try 
        {
            firstDate = DATEFORMAT.parse( "Sat Nov 09 11:40:23 EET 2013" )
                    .getTime();
        } 
        catch( ParseException e )
        {
            throw new RuntimeException(e);
        }
    }
    
    private long getTime( String date )
    {
        try 
        {
                return DATEFORMAT.parse( date )
                    .getTime();
        } 
        catch( ParseException e )
        {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void start( FlowProcess process, AggregatorCall call ) 
    {
        Map< Integer, Integer > buckets = new HashMap<>();
        int poss = ( int ) ( getTime( call.getArgumentFields().get(0)
                .toString() ) - firstDate );
        buckets.put( poss, 1 );
        call.setContext( buckets );
    }

    @Override
    public void aggregate( FlowProcess process, AggregatorCall call )
    {
        HashMap< Integer, Integer > buckets = (HashMap< Integer, Integer >)
                call.getContext();
        int poss = ( int ) ( getTime( call.getArgumentFields().get(0)
                .toString() ) - firstDate );
        
        if( buckets.containsKey( poss ) )
        {
            buckets.put( poss, buckets.get( poss ) + 1 );
        }
        else
        {
            buckets.put( poss, 1 );
        }
        
        call.setContext( buckets );
    }

    @Override
    public void complete( FlowProcess process, AggregatorCall call )
    {
        HashMap< Integer, Integer > buckets = (HashMap< Integer, Integer >)
                call.getContext();
        
        call.getOutputCollector().add( new Tuple( buckets ) );
    }

    
}
