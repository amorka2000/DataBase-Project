import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Page implements java.io.Serializable {
	
String tableName = "";
Vector Tuples = new Vector<Hashtable>();
Object max =null;
Object min = null;
String Path ="";
int PageNumber=0;
String key = "";
int MaxSize = 0;


public Page(int num,String TableName,String key){
	this.tableName= TableName;	
	this.Path = TableName+num+".ser";
	this.PageNumber = num;
	this.key = key;
	try {
	Properties test = new Properties();
	String filename = "DBApp.config";
	String workingDirectory = System.getProperty("user.dir");
	String abFilePath = "";
	abFilePath = workingDirectory + File.separator +"src\\main\\resources\\"+ filename;
	InputStream is = new FileInputStream(abFilePath);
	test.load(is);
	this.MaxSize = Integer.parseInt((String)test.get("MaximumRowsCountinPage"));
	
	}
	catch (IOException e) {
		e.printStackTrace();
	}
}
public void InsertIntoPage(Hashtable v) {          
	this.Tuples.add(v);
	this.sort();
	this.min = ((Hashtable)this.Tuples.get(0)).get(this.key);
	this.max = ((Hashtable)this.Tuples.get(this.Tuples.size()-1)).get(this.key);
	this.Serialize();
}



public void deleteWithKey(Object key) throws DBAppException {
	int x = BSearchInPage(key,0,this.Tuples.size()-1);
	if(x == -1) {
		throw new DBAppException("This ID doesn't exist in this table"); 
	}
	else {
		if(this.Tuples.size()==1) {
			String workingDirectory = System.getProperty("user.dir");
			String abFilePath = "";
			abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Pages\\"+ this.Path;
			File y = new File(abFilePath+this.Path);
			y.delete();
			return;
		}
		this.Tuples.remove(x);
		this.min = (((Hashtable)this.Tuples.get(0)).get(key));
		this.max = (((Hashtable)this.Tuples.get(this.Tuples.size()-1)).get(key));
		this.Serialize();
	}
}
public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
	int index = BSearchInPage(clusteringKeyValue,0,this.Tuples.size()-1);
	if(index == -1) {
		throw new DBAppException("This ID doesn't exist in this table");
	}
	else {
		Object[] keys = columnNameValue.keySet().toArray();
		String currentKey;
		for(int i =0;i<columnNameValue.size();i++) {
			currentKey = (String)keys[i];
			((Hashtable)this.Tuples.get(index)).replace(currentKey, columnNameValue.get(currentKey));
			this.Serialize();
		}
	}
}


public int BSearchInPage(Object key,int FirstIndex,int LastIndex) {
	int Half = 0;
	while(FirstIndex < LastIndex) {
		Half = ((LastIndex+FirstIndex)/2);
		if(this.Tuples.get(Half)==null) {
			
		}
		if(compareTo(key,((Hashtable)this.Tuples.get(Half)).get(this.key)) > 0) {
			FirstIndex = Half+1;
			}
		else if(compareTo(key,((Hashtable)this.Tuples.get(Half)).get(this.key)) < 0) {
			LastIndex=Half-1;
		}
		else if(compareTo(key,((Hashtable)this.Tuples.get(Half)).get(this.key)) == 0){
			return Half;
		}
	
	
	}
	if(FirstIndex == LastIndex) {
		if(compareTo(key,((Hashtable)this.Tuples.get(FirstIndex)).get(this.key)) == 0) {
				return FirstIndex;
		}
		else {
			return -1;
		}
	}
	else {
		return -1;
	}
}
public void deleteWithoutKey(Hashtable V) {
	Object key = "";
	Object value = null;
	ArrayList<Integer> deleted = new ArrayList<Integer>();
	for(int i = 0 ; i < V.size();i++) {
		key = ((Vector)V.keySet()).get(i);
		value = V.get(key);
		if(i==0) {
		for(int j = 0 ; j < this.Tuples.size();j++ ) {
			if(value.equals(((Hashtable)this.Tuples.get(j)).get(key))) {
					deleted.add(j);
				}
			}
		}
		else {
			for(int j =0 ;j <deleted.size();j++) {
				if(!(value.equals(((Hashtable)this.Tuples.get(deleted.get(j))).get(key)))){
					deleted.remove(j);
					j--;
				}
				
			}
		}
}
	for(int i =0 ; i < deleted.size();i++) {
		this.Tuples.remove(deleted.get(i));
	}
	this.min = ((Hashtable)this.Tuples.get(0)).get(this.key);
	this.max = ((Hashtable)this.Tuples.get(this.Tuples.size()-1)).get(this.key);
	this.Serialize();
}
public static int compareTo(Object a,Object b) {
	if(a instanceof Integer && b instanceof Integer) {
		if( ((Integer) a) < ((Integer) b)) {
			return -1;
		}
		if(((Integer) a) > ((Integer) b)) {
			return 1;
		}
		else {
			return 0;
		}
	}
	if(a instanceof String && b instanceof String){
		return ((String)a).compareTo((String)b);
	}
	if(a instanceof Double && b instanceof Double) {
		if(((Double) a) < ((Double)b)) {
			return -1;
		}
		if(((Double) a) > ((Double)b)) {
			return 1;
		}
		else {
			return 0;
		}
	}
	if(a instanceof Date && b instanceof Date) {
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		Date a1 = (Date) a;
		Date b1 = (Date) b;
		String aDateString = dateformat.format(a1);
		String bDateString = dateformat.format(b1);
		return (aDateString.compareTo(bDateString));
	}
	else {
	return 0;
	}
}
public void sort() {
	Hashtable newTuple = (Hashtable)this.Tuples.lastElement();
	if(!(this.Tuples.size()<2)) {
	for(int i = this.Tuples.size()-2;i>=0;i--) {
		if(this.Tuples.get(i) == null) {
			this.Tuples.setElementAt( newTuple,i);
			this.Tuples.setElementAt( null,i+1);
		}
		if(compareTo(((Hashtable) this.Tuples.get(i)).get(this.key),(newTuple.get(this.key)))>0) {
			Hashtable Temp = (Hashtable) this.Tuples.get(i);
			this.Tuples.setElementAt( newTuple,i);
			this.Tuples.setElementAt( Temp,i+1);
		}
		else {
			this.min = ((Hashtable)this.Tuples.get(0)).get(key);
			this.max = ((Hashtable)this.Tuples.lastElement()).get(key);
			this.Serialize();
			return;
		}
	}
	}
	if(this.PageNumber ==0) {
		this.min = ((Hashtable)this.Tuples.get(0)).get(key);
		this.max = ((Hashtable)this.Tuples.lastElement()).get(key);
		this.Serialize();
		return;
		
	}
	else {
		Page prev=deSerialize(this.tableName+(this.PageNumber-1)+".ser");
		if(compareTo(prev.max,newTuple.get(key))>0) {
			Hashtable Temp2 = (Hashtable) prev.Tuples.lastElement();
			prev.Tuples.setElementAt( this.Tuples.get(0),prev.Tuples.size()-1);
			this.Tuples.setElementAt( Temp2,0);
			prev.sort();
		}
	}
	this.min = ((Hashtable)this.Tuples.get(0)).get(key);
	this.max = ((Hashtable)this.Tuples.lastElement()).get(key);
	this.Serialize();
}


public void Serialize() {
	try {
		this.min = ((Hashtable)this.Tuples.get(0)).get(key);
		this.max = ((Hashtable)this.Tuples.lastElement()).get(key);
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Pages\\"+ this.Path;
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
public static Page deSerialize(String fileName) {
	try {
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Pages\\"+ fileName;
        FileInputStream fileIn = new FileInputStream(abFilePath);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Page e = (Page) in.readObject();
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
public ArrayList sqlSearch(Hashtable V) {
	Object key = "";
	Object value = null;
	ArrayList<Integer> result = new ArrayList<Integer>();
	ArrayList<String> result1 = new ArrayList<String>();
	for(int i = 0 ; i < V.size();i++) {
		key = (V.keySet().toArray())[i];
		value = V.get(key);
		if(i==0) {
		for(int j = 0 ; j < this.Tuples.size();j++ ) {
			if(value.equals(((Hashtable)this.Tuples.get(j)).get(key))) {
					result.add(j);
				}
			}
		}
		else {
			for(int j =0 ;j <result.size();j++) {
				if(!(value.equals(((Hashtable)this.Tuples.get(result.get(j))).get(key)))){
					result.remove(j);
					j--;
				}
				
			}
		}
}
	for(int k = 0 ; k<result.size();k++) {
		Hashtable x = (Hashtable)this.Tuples.get(result.get(k));
		result1.add(x.toString());
	}
	return result1;
}
public Hashtable get(int u) {
	return (Hashtable)this.Tuples.get(u);
}
}

