import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class GridIndex implements java.io.Serializable{
LinkedList<OneDimensional> Dimensions = new LinkedList<OneDimensional>();
String Name = "";
ArrayList<String> Buckets = new ArrayList<String>();
String[] Columns = null;
Table mainTable =null;
public GridIndex(String tableName,int num,String[] columnNames , ArrayList<String> min, ArrayList<String> max, ArrayList<String> type,Table T) throws DBAppException {
	this.Name = "G"+tableName+num;
	this.mainTable = T;
	this.Columns = columnNames;
	for(int i = 0 ; i < columnNames.length ; i++) {
		this.Dimensions.add(new OneDimensional(type.get(i),min.get(i),max.get(i),columnNames[i],T));
	}
	this.createPointers();
}
public void removePointer(Hashtable o) throws DBAppException {
	String bucket = "";
	for(int k = 0 ;k < this.Dimensions.size(); k++) {
		bucket = this.Dimensions.get(k).createPointers(o, bucket);
	}
	if(!bucket.equals("")) {
	if(this.Buckets.contains(bucket)) {
		Bucket b = deSerialize(this.Name+"B"+bucket);
		for(int i = 0 ; i < b.pointers.size() ; i++) {
			String[] c = b.pointers.get(i).split(",");
			Page p = Page.deSerialize(c[0]);
			int u = Integer.parseInt(c[1]);
			Hashtable bilo = p.get(u);
			if(bilo.equals(o)) {
				b.pointers.remove(i);
				return;
			}
		}
	}
}
}
public void Insert(Hashtable o,String PagePath,int row) throws DBAppException {
	String bucket = "";
	for(int k = 0 ;k < this.Dimensions.size(); k++) {
		bucket = this.Dimensions.get(k).createPointers(o, bucket);
	}
	if(!bucket.equals("")) {
	if(this.Buckets.contains(bucket)) {
		Bucket b = deSerialize(this.Name+"B"+bucket);
		b.addPointer(PagePath,row);
		b.Serialize();
	}
	else {
		this.Buckets.add(bucket);
		Bucket b = new Bucket(bucket,this.Name);
		b.addPointer(PagePath, row);
		b.Serialize();
	}
}
}
public void GridInsert(String page,int row,String tableName) throws IOException {
	int nameSize = tableName.length();
	int ser = tableName.length() - 4;
	Properties test = new Properties();
	String filename = "DBApp.config";
	String workingDirectory = System.getProperty("user.dir");
	String abFilePath = "";
	abFilePath = workingDirectory + File.separator +"src\\main\\resources\\"+ filename;
	InputStream is = new FileInputStream(abFilePath);
	test.load(is);
	int MaxSize = Integer.parseInt((String)test.get("MaximumRowsCountinPage"));
	for(int i = 0 ;i <this.Buckets.size();i++) {
		Bucket b = deSerialize(this.Name+"B"+this.Buckets.get(i));
		for(int j = 0 ; j <b.pointers.size();j++) {
			String[] c = b.pointers.get(i).split(",");
			int row1 = Integer.parseInt(c[1]);
			String page1 = c[0];
			if(page.compareTo(page1) > 0) {
				if((row1+1) == MaxSize) {
					int newPageNum = Integer.parseInt(((String)page1.subSequence(nameSize, ser)));
					newPageNum+=1;
					String newPage = tableName+newPageNum+".ser";
					int newRow = 0;
					String newPointer = newPage+","+newRow;
					b.pointers.remove(i);
					b.pointers.add(i,newPointer);
				}
				else {
					int newRow = row1 + 1;
					String newPointer = page1+","+newRow;
					b.pointers.remove(i);
					b.pointers.add(i,newPointer);
				}
			}
			else if(page.compareTo(page1) == 0) {
				if(row == row1) {
					if(row == MaxSize) {
						int newPageNum = Integer.parseInt(((String)page1.subSequence(nameSize, ser)));
						newPageNum+=1;
						String newPage = tableName+newPageNum+".ser";
						int newRow = 0;
						String newPointer = newPage+","+newRow;
						b.pointers.remove(i);
						b.pointers.add(i,newPointer);
					}
					else {
						int newRow = row1 + 1;
						String newPointer = page1+","+newRow;
						b.pointers.remove(i);
						b.pointers.add(i,newPointer);
					}
				}
				else if(row1 > row) {
					if(row1 == MaxSize) {
						int newPageNum = Integer.parseInt(((String)page1.subSequence(nameSize, ser)));
						newPageNum+=1;
						String newPage = tableName+newPageNum+".ser";
						int newRow = 0;
						String newPointer = newPage+","+newRow;
						b.pointers.remove(i);
						b.pointers.add(i,newPointer);
					}
					else {
						int newRow = row1 + 1;
						String newPointer = page1+","+newRow;
						b.pointers.remove(i);
						b.pointers.add(i,newPointer);
					}
				}
			}
		}
	}
}
public static Bucket deSerialize(String fileName) {
	try {
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Buckets\\"+ fileName;
        FileInputStream fileIn = new FileInputStream(abFilePath);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Bucket e = (Bucket) in.readObject();
        in.close();
        fileIn.close();
        return e;
     } catch (IOException i) {
    	 System.out.println("File not found");
        i.printStackTrace();
        return null;
     } catch (ClassNotFoundException c) {
        
        c.printStackTrace();
        return null;
     }
	
}
public void createPointers() throws DBAppException{
	
	for(int i = 0 ; i < this.mainTable.PageNumber; i++) {
		Page p = Page.deSerialize(this.mainTable.Name+i+".ser");
		for(int j = 0 ; j < p.Tuples.size(); j++) {
			String bucket = "";
			Hashtable o =((Hashtable)p.Tuples.get(j));
			for(int k = 0 ;k < this.Dimensions.size(); k++) {
				bucket = this.Dimensions.get(k).createPointers(o, bucket);
			}
			if(!bucket.equals("")) {
			if(this.Buckets.contains(bucket)) {
				Bucket b = deSerialize(this.Name+"B"+bucket);
				b.addPointer(p.Path,j);
				b.Serialize();
			}
			else {
				this.Buckets.add(bucket);
				Bucket b = new Bucket(bucket,this.Name);
				b.addPointer(p.Path, j);
				b.Serialize();
			}
		}
		}
	}
}
public void Serialize() {
	try {
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Grids\\"+ this.Name;
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
public ArrayList sqlSearchEquals(Hashtable v) throws DBAppException {
	ArrayList<String> bucketNums = new ArrayList<String>();
	String result = "";
	String column = (String)v.keySet().toArray()[0];
	for(int i =0 ;i < this.Dimensions.size();i++) {
		if(this.Dimensions.get(i).columnName.equals(column)) {
			result += this.Dimensions.get(i).createPointers(v, "");
			System.out.println(this.Dimensions.get(i).ranges+"Ranges");
		}
		else {
			result+="\\d";
		}
	}
	for(int i = 0 ; i<this.Buckets.size();i++) {
		if(this.Buckets.get(i).matches(result)) {
			bucketNums.add(this.Buckets.get(i));
		}
	}
	System.out.println(bucketNums+"  Equals");
	return bucketNums;
	
}
public ArrayList sqlSearchBigger(Hashtable v) throws DBAppException {
	ArrayList<String> bucketNums = new ArrayList<String>();
	String result = "";
	String column = (String)v.keySet().toArray()[0];
	String x = "";
	String JohnnyCash ="";
	for(int i =0 ;i < this.Dimensions.size();i++) {
		if(this.Dimensions.get(i).columnName.equals(column)) {
			x = this.Dimensions.get(i).createPointers(v, "");
			result += this.Dimensions.get(i).createPointers(v, "");
			JohnnyCash +="-";
		}
		else {
			result+="\\d";
			JohnnyCash +="_";
		}
	}
	for(int i = 0 ; i<this.Buckets.size();i++) {
		if(this.Buckets.get(i).matches(result)) {
			bucketNums.add(this.Buckets.get(i));
		}
	}
	int y = Integer.parseInt(x) + 1 ;
	for(int i=y;i<10;i++) {
		String Queen ="";
		for(int j = 0;j< JohnnyCash.length();j++) {
			if(JohnnyCash.charAt(j) == '-' ) {
				Queen +=i;
			}
			else {
				Queen +="\\d";
			}
		}
		for(int k = 0 ; k<this.Buckets.size();k++) {
			if(this.Buckets.get(k).matches(result)) {
				bucketNums.add(this.Buckets.get(k));
			}
	}
}
	return bucketNums;
}
public ArrayList sqlSearchLess(Hashtable v) throws DBAppException {
	ArrayList<String> bucketNums = new ArrayList<String>();
	String result = "";
	String column = (String)v.keySet().toArray()[0];
	String x = "";
	String JohnnyCash ="";
	for(int i =0 ;i < this.Dimensions.size();i++) {
		if((this.Dimensions.get(i).columnName).equals(column)) {
			x = this.Dimensions.get(i).createPointers(v, "");
			result += this.Dimensions.get(i).createPointers(v, "");
			JohnnyCash +="-";
		}
		else {
			result+="\\d";
			JohnnyCash +="_";
		}
	}
	for(int i = 0 ; i<this.Buckets.size();i++) {
		if(this.Buckets.get(i).matches(result)) {
			bucketNums.add(this.Buckets.get(i));
		}
	}
	int y = Integer.parseInt(x) + 1 ;
	for(int i=y;i>=0;i--) {
		String Queen ="";
		for(int j = 0;j< JohnnyCash.length();j++) {
			if(JohnnyCash.charAt(j) == '-' ) {
				Queen +=i;
			}
			else {
				Queen +="\\d";
			}
		}
		for(int k = 0 ; k<this.Buckets.size();k++) {
			if(this.Buckets.get(k).matches(result)) {
				bucketNums.add(this.Buckets.get(k));
			}
	}
}
	return bucketNums;
}
}
