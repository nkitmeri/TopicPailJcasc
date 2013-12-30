package data.jcascalog.classes;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.tuple.Tuple;
import cascalog.CascalogAggregator;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

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
            firstDate = DATEFORMAT.parse( "Thu Nov 28 14:24:00 EET 2013" )
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
        String date = ( String ) call.getArguments().getObject(1);
        int poss = ( int ) ( getTime( date ) - firstDate );
        buckets.put( poss, 1 );
        call.setContext( buckets );
    }

    @Override
    public void aggregate( FlowProcess process, AggregatorCall call )
    {
        HashMap< Integer, Integer > buckets = (HashMap< Integer, Integer >)
                call.getContext();
        String date = ( String ) call.getArguments().getObject(1);
        int poss = ( int ) ( getTime( date ) - firstDate );
        
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
