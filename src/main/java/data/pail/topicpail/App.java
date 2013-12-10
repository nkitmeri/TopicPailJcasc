package data.pail.topicpail;

import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.hadoop.Hfs;
import data.jcascalog.classes.CreateTopics;
import data.thrift.topicthrift.Topic;
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
        
        Subquery tokens = new Subquery( "?cleanTokens" )
                .predicate( Api.hfsTextline( args[1] ), "?tweets" )
                .predicate( new CreateTopics(), "?tweets" )
                .out( "?cleanTokens" );
        
        Api.execute( new Hfs(  new TextDelimited(), args[2] ), tokens );
        
        return 0;
    }
    
    /**
     * @param args
     * 
     */
    public static void main( String[] args ) 
    {
        Configuration conf = new Configuration();
        conf.set( "mapred.cache.files", args[0] ); // stopwords
        conf.set( "mapred.cache.files", args[1] ); // trends -> pos. 1-3
        conf.set( "mapred.cache.files", args[2] ); // trends -> pos. 4-10
        conf.set( "mapred.child.java.opts", "-Xmx2g" );
        try {
            ToolRunner.run(conf, new App(), args);
        } catch (Exception ex) {
            Logger.getLogger(App.class.getName())
                    .log(Level.SEVERE, null, ex);
        }
    }
    
}
