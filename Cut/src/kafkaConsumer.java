import java.io.File;
import java.util.Arrays;
import java.util.HashMap;  
import java.util.List;  
import java.util.Map;  
import java.util.Properties;

import javax.imageio.stream.FileImageOutputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.consumer.Consumer;  
import kafka.consumer.ConsumerConfig;  
import kafka.consumer.ConsumerIterator;  
import kafka.consumer.KafkaStream;  
import kafka.javaapi.consumer.ConsumerConnector;  
  
  
  

public class kafkaConsumer extends Thread{  
  
    private String topic;  
      
    public kafkaConsumer(String topic){  
        super();  
        this.topic = topic;  
    }  
      
      
    @Override  
    public void run() {  
    	Properties properties = new Properties(); 
        properties.put("bootstrap.servers", "rman-nod1:9092");
        properties.put("group.id", "example");
        properties.put("host.name", "rman-nod1");
        properties.put("broker.id","0");
        properties.put("producer.type", "sync");
        properties.put("request.required.acks", "1");
        properties.put("compression.codec", "gzip");
        properties.put("zookeeper.connect", "rman-nod1:2181");
        
        properties.put(
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); 
        properties.put(
                "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test"));
        
        System.out.println("Receiving commands...");
        int i=1,nn,pn,ps,max;
        long startTime=0,endTime=0;
        byte[][] result=new byte[1000][] ; 
        /*for (int j = 0; j < 99; ++j) {
        	result[j] =n;
        }*/
        int[] symbol=new int[1000];
        byte[] inter=null;
        int[] length=new int[1000];
        int j=0;
        int lo;
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(10);
            for (ConsumerRecord<String, byte[]> record : records) {
            
            	inter=record.value();
            	//System.out.println(inter.length);
            	//System.out.println(Arrays.toString(inter));
            	if(inter.length<21||(inter[12]!=3||inter[13]!=0||inter[14]!=3||inter[15]!=4))
            		continue;
            	if(i==1){
            		i=0;
            		startTime=System.nanoTime();
            	}
            	nn=inter[0]*1000+inter[1]*100+inter[2]*10+inter[3];
            	pn=inter[4]*1000+inter[5]*100+inter[6]*10+inter[7];
            	ps=(inter[8] & 0xff) | ((inter[9] << 8) & 0xff00) // | 表示安位或   
        				| ((inter[10] << 24) >>> 8) | (inter[11] << 24);
            	max=(inter[16] & 0xff) | ((inter[17] << 8) & 0xff00) // | 表示安位或   
        				| ((inter[18] << 24) >>> 8) | (inter[19] << 24);
            	//System.out.println(max);
            	if(symbol[nn]==0)
            	{
            		result[nn]=new byte[max];
            		symbol[nn]=1;
            		//System.out.println(result[nn].length);
            		//System.out.println(Arrays.toString(result[nn]));
            	}
            	for(j=20;j<inter.length;j++)
            	{
            		//System.out.println(Arrays.toString(result));
            		result[nn][pn*10220+j-20]=inter[j];
            	}
            	lo=pn*10224+inter.length-1;
            	if((++length[nn])==ps)
            		{
            			//System.out.println(Arrays.toString(result[nn]));
            			byte2image(result[nn],"/home/labadmin/zhouyang/res.jpg");
            			break;
            		}
            	
            	//System.out.println(Arrays.toString(record.value()));

            	endTime=System.nanoTime();
            	System.out.println((endTime-startTime));
            	}

            }
    }  
    public void byte2image(byte[] data,String pat){
	    if(data.length<3||pat.equals("")) return;
	    try{
	    FileImageOutputStream imageOutput = new FileImageOutputStream(new File(pat));
	    imageOutput.write(data, 0, data.length);
	    imageOutput.close();
	    //System.out.println("Make Picture success,Please find image in " + pat);
	    //System.out.println(Arrays.toString(data));
	    //System.out.println(data.length);
	    } catch(Exception ex) {
	      System.out.println("Exception: " + ex);
	      ex.printStackTrace();
	    }
	  }
      
    public static void main(String[] args) {  
        new kafkaConsumer("test").start();// 使用kafka集群中创建好的主题 test   
        System.out.println("test test test test test");  
    }  
       
} 