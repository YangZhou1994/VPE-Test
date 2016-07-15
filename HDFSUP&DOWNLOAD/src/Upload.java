import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.io.IOException;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;


public class Upload {

  public static void main(String[] args) throws URISyntaxException{   
	  try{
		  byte[] data=null;
		  String local="/home/labadmin/zhouyang/video/test.mp4";//  
		  String hdfs="hdfs://rman-nod1:8020/mnt/disk1/zhouyang/video/test.mp4";
		  Configuration conf=new Configuration();
		  FileSystem fileS=FileSystem.get(new URI(hdfs), conf);
		  FileSystem localFile =FileSystem.getLocal(conf);
		  Path input=new Path(local);
		  Path out=new Path(hdfs);
		  
		  //FSDataInputStream localInStream=.open(input);
		  //FSDataOutputStream outStream=fileS.create(output);
		  

		  FileStatus[] inputFile=localFile.listStatus(input);
		  FSDataOutputStream outStream=fileS.create(out);
		  for(int i=0;i<inputFile.length;i++)
		  {
			  System.out.println(inputFile[i].getPath().getName());
			  FSDataInputStream in=localFile.open(inputFile[i].getPath());
			  
			  byte[] buffer=new byte[1024];
			  int bytesRead=0;
			  while((bytesRead=in.read(buffer))!=-1)
			  {
				  //System.out.println(buffer);
				  outStream.write(buffer,0,bytesRead);
			  }
			  //data = output.toByteArray();
			  //System.out.println(Arrays.toString(data));
			  in.close();
		  }
		  }
		  catch (FileNotFoundException ex1) {
	      ex1.printStackTrace();
		  }
		  catch (IOException ex1) {
		      ex1.printStackTrace();
		    }
	          

  }       
}
