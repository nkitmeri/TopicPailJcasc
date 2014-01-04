
package data.jcascalog.classes;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author nikos
 */
public class CreateBucketsBuf extends CascalogBuffer {

    private static final DateFormat DATEFORMAT =
                    new SimpleDateFormat( "EEE MMM dd HH:mm:ss ZZZZZ yyyy" );
    private static final long TO_MIN = 60 * 2 * 1000;
    private static long firstDate;
    private String date = "";
    private static final HashMap< Long, Integer > buckets = new HashMap();
    private static final Tuple tuple = new Tuple(1);
    private static long tmp;
    private static long poss;
    private static Iterator<TupleEntry> iter;
    private static TupleEntry t;
    
    static
    {
        try 
        {
            firstDate = DATEFORMAT.parse( "Mon Nov 11 14:24:07 EET 2013" )
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
    public void operate(FlowProcess proccess, BufferCall call) {
//        buckets = new HashMap<>();
        iter = call.getArgumentsIterator();
//        TupleEntry t;
                
        while(iter.hasNext()) 
        {
            t = iter.next();
            date = t.getTuple().toString();
            tmp = getTime( date );
            poss = ( tmp - firstDate ) / TO_MIN;
            
            if( buckets.containsKey( poss ) )
            {
                buckets.put( poss, buckets.get( poss ) + 1 );
            }
            else
            {
                buckets.put( poss, 1 );
            }
        }
        
        tuple.set( 0, buckets );
        call.getOutputCollector().add( tuple );
        buckets.clear();
    }
    
}
