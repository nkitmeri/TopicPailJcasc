package data.pail.app;

import com.backtype.hadoop.pail.PailStructure;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

/**
 *
 * @author nikos
 */
public abstract class ThriftPailStructure<T extends Comparable> implements
		PailStructure<T> {
    private transient TSerializer ser;
    private transient TDeserializer des;
    
    private TSerializer getSerializer() {
        if(ser==null) ser = new TSerializer();
        return ser;
    }
    
    private TDeserializer getDeserializer() {
        if(des==null) des = new TDeserializer();
        return des;
    }
    
    @Override
    public byte[] serialize(T obj) {
        try {
            return getSerializer().serialize((TBase)obj);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public T deserialize(byte[] record) {
        T ret = createThriftObject();
        
        try {
            getDeserializer().deserialize((TBase)ret, record);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
    
    protected abstract T createThriftObject();

}
