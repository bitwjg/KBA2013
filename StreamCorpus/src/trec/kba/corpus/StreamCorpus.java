package trec.kba.corpus;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import kba.StreamItem;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class StreamCorpus {

	/*read streamitem(document) list from xz compressed file
	 * return document list in the xz file
	 */
	public List<StreamItem> readStreamItem(String filepath) throws IOException, TException
	{
		File file = new File(filepath);
    	InputStream input = new FileInputStream(file);    	
    	XZCompressorInputStream stream = new XZCompressorInputStream(input); 	
    	TTransport transport;
    	transport = new TIOStreamTransport(stream);
    	transport.open();  	
    	TProtocol protocol = new TBinaryProtocol(transport);
    	List<StreamItem> doc_list = new ArrayList<StreamItem>();
    	while(true)
    	{
    		StreamItem doc = new StreamItem(); 
            try { 
                doc.read(protocol); 
            } catch (TTransportException e) { 
                if (e.getType() == TTransportException.END_OF_FILE) 
                { 
                    break; 
                } 
            } 
            doc_list.add(doc);
    	}
    	transport.close();
    	return doc_list;
	}
	
	public static void main(String[] args)
	{
		StreamCorpus streamcorpus = new StreamCorpus();
		String curdir = System.getProperty("user.dir");
		String filepath = curdir + "\\CLASSIFIED-4-3c995e711dc882945149394c83529f6d-cd51adbe1fb4401fb300085654e11195.sc.xz";
		try{
			List<StreamItem> doc_list = streamcorpus.readStreamItem(filepath);
			System.out.println("the # of docs in xz file: " + doc_list.size());
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
}
