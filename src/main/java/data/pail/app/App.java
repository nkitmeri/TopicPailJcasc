package data.pail.app;

import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;
import com.twitter.maple.tap.StdoutTap;
import data.jcascalog.queries.Queries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import jcascalog.Api;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author nikos
 */
public class App extends Configured implements Tool {
    
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
        
//        Api.execute( new Hfs(  new TextDelimited(), args[4] ), 
//                new Queries( args[0], "?cleanTokens, ?isTrend, !timeTrended" )
//                        .getQuery() );
        
        Api.execute( new Hfs(  new TextDelimited(), args[4] ),//new StdoutTap(), ;
                new Queries( args[0], "?cleanTokens, ?isTrend,"
                        + " !timeTrended, ?timeBuckets" )
                        .getQuery() );
        
//        Api.execute( new Hfs(  new TextDelimited(), args[4] ), 
//                new Queries( args[0], "?cleanTokens, ?isTrend,"
//                        + " !timeTrended, ?tweetTime" )
//                        .getQuery() );
        
        return 0;
    }
    
    /**
     * @param args
     * 
     */
    public static void main( String[] args ) 
    {
        Configuration conf = new Configuration();
        conf.setClass( "App", App.class, Tool.class);
        String files = args[1] + "," + args[2] + "," + args[3];
        conf.set( "mapred.cache.files", files ); // stopwords
        conf.set( "mapred.reduce.tasks", "28" );
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.output.compression.type", "BLOCK"); 
        conf.set("mapred.map.output.compression.codec", 
                "org.apache.hadoop.io.compress.GzipCodec");
        conf.set( "mapred.child.java.opts", "-Xmx1g" );
        
        try {
            ToolRunner.run(conf, new App(), args);
        } catch (Exception e) {
//            Logger.getLogger(App.class.getName())
//                    .log(Level.SEVERE, null, e);
            e.printStackTrace( System.out );
            throw new RuntimeException(e);
        }
    }
    
}
