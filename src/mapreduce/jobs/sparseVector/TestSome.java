package mapreduce.jobs.sparseVector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by liruibo on 2017/8/9.
 */
public class Test {
    @org.junit.Test
    public void testDataPath(){
        try {
            InputStream ips = Test.class.getClassLoader().getResourceAsStream("mapreduce/jobs/sparseVector/input");
            byte[] buffer = new byte[1024];
            int num = ips.read(buffer);
            System.out.print(buffer.toString());
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
