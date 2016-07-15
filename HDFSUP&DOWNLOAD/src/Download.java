import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
  
  
public class Download{  
  
    public static void main(String[] args) {  
        try {  
        String dsf = "hdfs://rman-nod1:8020/mnt/disk1/zhouyang/video/test.mp4";  
        //String local="/home/labadmin/zhouyang/video/result.mp4";
        Configuration conf = new Configuration();  
          
        FileSystem fs = FileSystem.get(URI.create(dsf),conf);          
        //FileSystem localFile =FileSystem.getLocal(conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dsf));  
        //Path out=new Path(local);
        //FSDataOutputStream outStream=localFile.create(out);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] ioBuffer = new byte[1024];  
        byte[] data=null;
        int readLen = hdfsInStream.read(ioBuffer);  
        while(readLen!=-1)  
        {  
            //System.out.write(ioBuffer, 0, readLen);
            //System.out.println(Arrays.toString(ioBuffer));
        	//outStream.write(ioBuffer,0,readLen);
        	output.write(ioBuffer, 0, readLen);
        	readLen = hdfsInStream.read(ioBuffer);  
        }  
        data = output.toByteArray();
        System.out.println(Arrays.toString(data));
        hdfsInStream.close();  
        fs.close();  
        } catch (IOException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
        }  
          
    }  
  
}
