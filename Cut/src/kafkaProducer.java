import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;  
import java.util.concurrent.TimeUnit;

import javax.imageio.stream.FileImageInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import kafka.serializer.StringEncoder;  
  
  
  
  
public class kafkaProducer extends Thread{  
  
	long startTime=System.nanoTime();
	private String topic;  
      
    public kafkaProducer(String topic){  
        super();  
        this.topic = topic;  
    }  
      
      
    @SuppressWarnings("null")
	@Override  
    public void run() {  
    	 Properties properties = new Properties(); 
    	 properties.put("zookeeper.connect", "rman-nod1:2181");
         properties.put("host.name", "rman-nod1");
         properties.put("broker.id","0");
         properties.put("bootstrap.servers", "rman-nod1:9092");
         properties.put("producer.type", "sync");
         properties.put("request.required.acks", "1");
         properties.put("compression.codec", "gzip");
         properties.put(
                 "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
         properties.put(
                 "value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
         KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
         for (int i = 0; i < 1000	; i++) {
             byte[] note=image2byte();
             long time=System.nanoTime();
             int stop;
             int max=note.length;
            
             if(max%10240==0)
             {
            	 stop=max/10240;
             }
             else
            	 stop = max/10240+1;
             ////
             

             int c=0;
             int n=0;
             for(n=0;n<stop-1;n++){
            	 byte[] sendout=new byte[10240];
            	 sendout[0]=(byte) (i/1000);
            	 sendout[1]=(byte) (i/100-sendout[0]*10);
            	 sendout[2]=(byte) (i/10-sendout[0]*100-sendout[1]*10);
            	 sendout[3]=(byte) (i%10);
            	 sendout[4]=(byte) (n/1000);
                 sendout[5]=(byte) (n/100-sendout[4]*10);
                 sendout[6]=(byte) (n/10-sendout[4]*100-sendout[5]*10);
                 sendout[7]=(byte) (n%10);
                 int length=note.length/10240+1;
         		 sendout[8] = (byte) (length & 0xff);// 最低位   
        		 sendout[9] = (byte) ((length >> 8) & 0xff);// 次低位   
        		 sendout[10] = (byte) ((length >> 16) & 0xff);// 次高位   
        		 sendout[11] = (byte) (length >>> 24);// 最高位,无符号右移。
        		 sendout[12]=3;
        		 sendout[13]=0;
        		 sendout[14]=3;
        		 sendout[15]=4;
         		 sendout[16] = (byte) (max & 0xff);// 最低位   
        		 sendout[17] = (byte) ((max >> 8) & 0xff);// 次低位   
        		 sendout[18] = (byte) ((max >> 16) & 0xff);// 次高位   
        		 sendout[19] = (byte) (max >>> 24);// 最高位,无符号右移。
        		 
	             for(int j=20;j<10240;j++){
	            	 sendout[j]=note[c++];
	            	 if(c>=max)
	            		 break;
	             }
	             //System.out.println(Arrays.toString(sendout));
	             producer.send(new ProducerRecord<String, byte[]>(topic,sendout));
	             sendout=null;
             }        
             //System.out.println(max);
             n=stop-1;
             byte[] sendout=new byte[max-(n)*10240];
        	 sendout[0]=(byte) (i/1000);
        	 sendout[1]=(byte) (i/100-sendout[0]*10);
        	 sendout[2]=(byte) (i/10-sendout[0]*100-sendout[1]*10);
        	 sendout[3]=(byte) (i%10);
        	 sendout[4]=(byte) (n/1000);
             sendout[5]=(byte) (n/100-sendout[4]*10);
             sendout[6]=(byte) (n/10-sendout[4]*100-sendout[5]*10);
             sendout[7]=(byte) (n%10);
             int length=note.length/10240+1;
     		 sendout[8] = (byte) (length & 0xff);// 最低位   
    		 sendout[9] = (byte) ((length >> 8) & 0xff);// 次低位   
    		 sendout[10] = (byte) ((length >> 16) & 0xff);// 次高位   
    		 sendout[11] = (byte) (length >>> 24);// 最高位,无符号右移。
    		 sendout[12]=3;
    		 sendout[13]=0;
    		 sendout[14]=3;
    		 sendout[15]=4;
     		 sendout[16] = (byte) (max & 0xff);// 最低位   
    		 sendout[17] = (byte) ((max >> 8) & 0xff);// 次低位   
    		 sendout[18] = (byte) ((max >> 16) & 0xff);// 次高位   
    		 sendout[19] = (byte) (max >>> 24);// 最高位,无符号右移。
             for(int j=20;j<sendout.length;j++){
            	 sendout[j]=note[c++];
            	 if(c>=max)
            		 break;
             }
             //System.out.println(Arrays.toString(sendout));
             producer.send(new ProducerRecord<String, byte[]>(topic,sendout));
             sendout=null;
             
         }
         long endTime=System.nanoTime();
         System.out.println((endTime-startTime));

    }
	public byte[] image2byte(){
	    byte[] data = null;
	    String path="/home/labadmin/zhouyang/1.jpg";
	    FileImageInputStream input = null;
	    try {
	      input = new FileImageInputStream(new File(path));
	      ByteArrayOutputStream output = new ByteArrayOutputStream();
	      byte[] buf = new byte[1024];
	      int numBytesRead = 0;
	      while ((numBytesRead = input.read(buf)) != -1) {
	      output.write(buf, 0, numBytesRead);	      
	      }    
	      data = output.toByteArray();   
	      //System.out.println(Arrays.toString(data));
	      output.close();
	      input.close();
	    }
	    catch (FileNotFoundException ex1) {
	      ex1.printStackTrace();
	    }
	    catch (IOException ex1) {
	      ex1.printStackTrace();
	    }

	    return data;
	  } 
    public static void main(String[] args) {  
        new kafkaProducer("test").start();// 使用kafka集群中创建好的主题 test   
         System.out.println("test test test test test");  
    }  
       
}  
