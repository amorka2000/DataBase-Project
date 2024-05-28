import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class Bucket implements java.io.Serializable{
ArrayList<String> pointers = new ArrayList<String>();
int MaxSize = 0;
String Name = "";
public Bucket(String num,String GName) {
	this.Name = GName+"B"+num;
	try {
	Properties test = new Properties();
	String filename = "DBApp.config";
	String workingDirectory = System.getProperty("user.dir");
	String abFilePath = "";
	abFilePath = workingDirectory + File.separator +"src\\main\\resources\\"+ filename;
	InputStream is = new FileInputStream(abFilePath);
	test.load(is);
	this.MaxSize = Integer.parseInt((String)test.get("MaximumKeysCountinIndexBucket"));
	}
	catch (IOException e) {
		e.printStackTrace();
	}
}
public void Serialize() {
	try {
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Buckets\\"+ this.Name;
        FileOutputStream fileOut = new FileOutputStream(abFilePath);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
     } catch (IOException i) {
    	 System.out.println("File not found");
        i.printStackTrace();
     }
}
public void addPointer(String pageName, int tupleRow) {
	String x = pageName +","+ tupleRow;
	this.pointers.add(x);
	
}
}
