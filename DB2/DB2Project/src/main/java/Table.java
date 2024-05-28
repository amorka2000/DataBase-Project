import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Table implements java.io.Serializable  {
String Name ="";	
int PageNumber =0;
String Key ="";
int GridNumber =0;


public Table(String Name, String Key) {
	this.Name=Name;
	this.Key = Key;
}

public static int compareTo(Object a,Object b) {
	if(a instanceof Integer && b instanceof Integer) {
		if( ((Integer) a) < ((Integer) b) ) {
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
public void Insert(Hashtable V) throws IOException, DBAppException {
	if(this.GridNumber==0) {
	if(this.PageNumber==0) {
		Page newPage = new Page(this.PageNumber,this.Name,this.Key);
		this.PageNumber++;
		newPage.InsertIntoPage(V);
		return;
	}
	else {
	int pagenum = this.PageNumber - 1;
	Page P=(Page)deSerialize(this.Name+pagenum+".ser");
	if(P.Tuples.size()< P.MaxSize) {
		P.InsertIntoPage(V);
		return;
	}
	else {
		Page newPage = new Page(this.PageNumber,this.Name,this.Key);
		this.PageNumber++;
		newPage.InsertIntoPage(V);
		return;
	}
	}
	}
	else {
		ArrayList<GridIndex> grids = new ArrayList<GridIndex>();
		for(int i = 0;i < this.GridNumber ; i++) {
			GridIndex g = deSerializeGrid("G"+this.Name+i);
			grids.add(g);
		}
		if(this.PageNumber==0) {
			Page newPage = new Page(this.PageNumber,this.Name,this.Key);
			this.PageNumber++;
			newPage.InsertIntoPage(V);
			
		}
		else {
		int pagenum = this.PageNumber - 1;
		Page P=(Page)deSerialize(this.Name+pagenum+".ser");
		if(P.Tuples.size()< P.MaxSize) {
			P.InsertIntoPage(V);
			
		}
		else {
			Page newPage = new Page(this.PageNumber,this.Name,this.Key);
			this.PageNumber++;
			newPage.InsertIntoPage(V);
			
		}
		}
		int pagenumber = BSearch(V.get(this.Key),0,this.PageNumber-1);
		Page P=(Page)deSerialize(this.Name+pagenumber+".ser");
		int row = P.BSearchInPage(V.get(this.Key), 0, P.Tuples.size()-1);
		String page = this.Name+pagenumber+".ser";
		for(int i = 0 ; i < grids.size();i++) {
			grids.get(i).GridInsert(page, row, this.Name);
			grids.get(i).Insert(V, page, row);
			grids.get(i).Serialize();
		}
	}
}
public void Delete(Hashtable V) throws DBAppException {
	if(this.GridNumber==0) {
	if(this.PageNumber== 0) {
		throw new DBAppException("This table has no tuples");
	}
	else {
	boolean flag = false;
	Object key = null;
		if(V.containsKey(Key)) {
			flag = true;	
		}
	
	if(flag) {//Delete with key
		key = V.get(Key);
		int y = BSearch(key,0,this.PageNumber-1);
		if(y == -1) {
			throw new DBAppException("This ID doesn't exist in this table");
		}
		else {
			Page p = deSerialize(this.Name+y+".ser");
			p.deleteWithKey(key);
		}
	}
	else {//Delete without key
		for(int i = 0 ; i < this.PageNumber;i++) {
		Page p = deSerialize(this.Name+i+".ser");
		p.deleteWithoutKey(V);
		}
	}
	}
	}
	else {
		ArrayList<Hashtable> columns = new ArrayList<Hashtable>();
		for(int i = 0 ; i < V.size();i++) {
			Hashtable v = new Hashtable();
			v.put(V.keySet().toArray()[i], V.get(V.keySet().toArray()[i]));
			columns.add(v);
		}
		ArrayList<Hashtable> tuples = new ArrayList<Hashtable>();
		for(int i = 0 ; i < columns.size();i++) {
			GridIndex g =null;
			Boolean flag = false;
			for(int k = 0 ;k < this.GridNumber ;k++) {
				g = deSerializeGrid("G"+this.Name+k);
				for(int j = 0;j<g.Columns.length;j++) {
					if(columns.get(i).keySet().toArray()[0].equals(g.Columns[j])) {
						flag = true;
						break;
					}
				}
				if(flag) {
					break;
				}
			}
			if(!flag) {
				int x=this.GridNumber;
				this.GridNumber = 0 ;
				this.Delete(V);
				this.GridNumber = x;
				return;
			}
			else {
				ArrayList<String> bucketnum = (g.sqlSearchEquals(columns.get(i)));
				for(int j = 0 ; j < bucketnum.size();j++ ) {
					Bucket b = deSerializeBucket(g.Name+"B"+bucketnum.get(j));
					for(int k =0;k< b.pointers.size();k++) {
						String[] c = b.pointers.get(k).split(",");
						Page p = deSerialize(c[0]);
						int u = Integer.parseInt(c[1]);
						tuples.add(p.get(u));
					}
				}
			}
		}
		ArrayList<Hashtable> result = new ArrayList<Hashtable>();
		for(int i = 0 ; i < tuples.size();i++) {
			int x = Collections.frequency(tuples, tuples.get(i));
			if(x == V.size()) {
				if(!(result.contains(tuples.get(i)))) {
					result.add(tuples.get(i));
				}
			}
		}
		int x = this.GridNumber;
		this.GridNumber = 0;
		GridIndex g = null;
		for(int i = 0;i < result.size();i++) {
			this.Delete(result.get(i));
			for(int k = 0 ;k<x;k++) {
				g = deSerializeGrid("G"+this.Name+k);
				g.removePointer(result.get(i));
				g.Serialize();
			}
		}
		this.GridNumber = x;
		
	}
}
public ArrayList sqlSearchLess(Hashtable v) throws DBAppException {
	ArrayList<Hashtable> result1 = new ArrayList<Hashtable>();
	ArrayList<String> result = new ArrayList<String>();
	if(this.GridNumber==0) {
		for(int  i = 0 ;i < this.PageNumber ;i++) {
			Page p = deSerialize(this.Name+i+".ser");
			for(int j = 0 ; j < p.Tuples.size();j++) {
				if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))<0) {
				result.add(((Hashtable)p.Tuples.get(j)).toString());
				}
			}
		}
		return result;
	}
	else {
		GridIndex g =null;
		Boolean flag =false;
		for(int i = 0 ;i < this.GridNumber ;i++) {
			g = deSerializeGrid("G"+this.Name+i);
			for(int j = 0;j<g.Columns.length;j++) {
				if(v.keySet().toArray()[0].equals(g.Columns[j])) {
					flag = true;
					break;
				}
			}
			if(flag) {
				break;
			}
		}
		if(flag) {
		ArrayList<String> GridResults = g.sqlSearchLess(v);
		
		for(int  i =0 ;i<GridResults.size();i++) {
			
			Bucket b = deSerializeBucket(g.Name+"B"+GridResults.get(i));
			for(int j =0;j< b.pointers.size();j++) {
				String[] c = b.pointers.get(j).split(",");
				Page p = deSerialize(c[0]);
				int u = Integer.parseInt(c[1]);
				result1.add(p.get(u));
			}
		}
		
		for(int i=0;i<result1.size();i++) {
			if(!(compareTo((result1.get(i)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))<0)) {
				result1.remove(i);
				i--;
			}
		}
		for(int i = 0;i<result1.size();i++) {
			result.add(result1.get(i).toString());
		}
		return result;
	}
		else {
			for(int  i = 0 ;i < this.PageNumber ;i++) {
				Page p = deSerialize(this.Name+i+".ser");
				for(int j = 0 ; j < p.Tuples.size();j++) {
					if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))<0) {
					result.add(((Hashtable)p.Tuples.get(j)).toString());
					}
				}
			}
			return result;
		}
	}
}
public ArrayList sqlSearchLessEqual(Hashtable v) throws DBAppException {
	ArrayList<Hashtable> result1 = new ArrayList<Hashtable>();
	ArrayList<String> result = new ArrayList<String>();
	if(this.GridNumber==0) {
		for(int  i = 0 ;i < this.PageNumber ;i++) {
			Page p = deSerialize(this.Name+i+".ser");
			for(int j = 0 ; j < p.Tuples.size();j++) {
				if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))<=0) {
				result.add(((Hashtable)p.Tuples.get(j)).toString());
				}
			}
		}
		return result;
	}
	else {
		GridIndex g =null;
		Boolean flag =false;
		for(int i = 0 ;i < this.GridNumber ;i++) {
			g = deSerializeGrid("G"+this.Name+i);
			for(int j = 0;j<g.Columns.length;j++) {
				if(((String)v.keySet().toArray()[0]).equals(g.Columns[j])) {
					flag = true;
					break;
				}
			}
			if(flag) {
				break;
			}
		}
		if(flag) {
		ArrayList<String> GridResults = g.sqlSearchLess(v);
		for(int  i =0 ;i<GridResults.size();i++) {
			
			Bucket b = deSerializeBucket(g.Name+"B"+GridResults.get(i));
			for(int j =0;j< b.pointers.size();j++) {
				String[] c = b.pointers.get(j).split(",");
				Page p = deSerialize(c[0]);
				int u = Integer.parseInt(c[1]);
				result1.add(p.get(u));
			}
		}
		System.out.println(result1+"Before filter");
		for(int i=0;i<result1.size();i++) {
			if(!(compareTo((result1.get(i)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))<=0)) {
				result1.remove(i);
				i--;
			}
		}
		System.out.println(result1+"After Filter");
		for(int i = 0;i<result1.size();i++) {
			result.add(result1.get(i).toString());
		}
		return result;
		}
		else {
			for(int  i = 0 ;i < this.PageNumber ;i++) {
				Page p = deSerialize(this.Name+i+".ser");
				for(int j = 0 ; j < p.Tuples.size();j++) {
					if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))<=0) {
					result.add(((Hashtable)p.Tuples.get(j)).toString());
					}
				}
			}
			return result;
		}
	}
}
public ArrayList sqlSearchBigger(Hashtable v) throws DBAppException {
	ArrayList<Hashtable> result1 = new ArrayList<Hashtable>();
	ArrayList<String> result = new ArrayList<String>();
	if(this.GridNumber==0) {
		for(int  i = 0 ;i < this.PageNumber ;i++) {
			Page p = deSerialize(this.Name+i+".ser");
			for(int j = 0 ; j < p.Tuples.size();j++) {
				if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))>0) {
				result.add(((Hashtable)p.Tuples.get(j)).toString());
				}
			}
		}
		return result;
	}
	else {
		GridIndex g =null;
		Boolean flag = false;
		for(int i = 0 ;i < this.GridNumber ;i++) {
			g = deSerializeGrid("G"+this.Name+i);
			for(int j = 0;j<g.Columns.length;j++) {
				if(v.keySet().toArray()[0].equals(g.Columns[j])) {
					flag = true;
					break;
				}
			}
			if(flag) {
				break;
			}
		}
		if(flag) {
		ArrayList<String> GridResults = g.sqlSearchBigger(v);
		for(int  i =0 ;i<GridResults.size();i++) {
			
			Bucket b = deSerializeBucket(g.Name+"B"+GridResults.get(i));
			for(int j =0;j< b.pointers.size();j++) {
				String[] c = b.pointers.get(j).split(",");
				Page p = deSerialize(c[0]);
				int u = Integer.parseInt(c[1]);
				result1.add(p.get(u));
			}
		}
		for(int i=0;i<result1.size();i++) {
			if(!(compareTo((result1.get(i)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))>0)) {
				result1.remove(i);
				i--;
			}
		}
		for(int i = 0;i<result1.size();i++) {
			result.add(result1.get(i).toString());
		}
		return result;
	}
		else {
			for(int  i = 0 ;i < this.PageNumber ;i++) {
				Page p = deSerialize(this.Name+i+".ser");
				for(int j = 0 ; j < p.Tuples.size();j++) {
					if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))>0) {
					result.add(((Hashtable)p.Tuples.get(j)).toString());
					}
				}
			}
			return result;
		}
	}
}
public ArrayList sqlSearchBiggerEqual(Hashtable v) throws DBAppException {
	ArrayList<Hashtable> result1 = new ArrayList<Hashtable>();
	ArrayList<String> result = new ArrayList<String>();
	if(this.GridNumber==0) {
		for(int  i = 0 ;i < this.PageNumber ;i++) {
			Page p = deSerialize(this.Name+i+".ser");
			for(int j = 0 ; j < p.Tuples.size();j++) {
				if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))>=0) {
				result.add(((Hashtable)p.Tuples.get(j)).toString());
				}
			}
		}
		return result;
	}
	else {
		GridIndex g =null;
		Boolean flag = false;
		for(int i = 0 ;i < this.GridNumber ;i++) {
			g = deSerializeGrid("G"+this.Name+i);
			for(int j = 0;j<g.Columns.length;j++) {
				if(v.keySet().toArray()[0].equals(g.Columns[j])) {
					flag = true;
					break;
				}
			}
			if(flag) {
				break;
			}
		}
		if(flag) {
		ArrayList<String> GridResults = g.sqlSearchBigger(v);
		for(int  i =0 ;i<GridResults.size();i++) {
			
			Bucket b = deSerializeBucket(g.Name+"B"+GridResults.get(i));
			for(int j =0;j< b.pointers.size();j++) {
				String[] c = b.pointers.get(j).split(",");
				Page p = deSerialize(c[0]);
				int u = Integer.parseInt(c[1]);
				result1.add(p.get(u));
			}
		}
		for(int i=0;i<result1.size();i++) {
			if(!(compareTo((result1.get(i)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))>=0)) {
				result1.remove(i);
				i--;
			}
		}
		for(int i = 0;i<result1.size();i++) {
			result.add(result1.get(i).toString());
		}
		return result;
	}
		else {
			for(int  i = 0 ;i < this.PageNumber ;i++) {
				Page p = deSerialize(this.Name+i+".ser");
				for(int j = 0 ; j < p.Tuples.size();j++) {
					if(compareTo(((Hashtable)p.Tuples.get(j)).get(v.keySet().toArray()[0]),v.get(v.keySet().toArray()[0]))>=0) {
					result.add(((Hashtable)p.Tuples.get(j)).toString());
					}
				}
			}
			return result;
		}
	}
}
public ArrayList sqlSearchNotEquals(Hashtable v) {
	ArrayList<Hashtable> result = new ArrayList<Hashtable>();
	ArrayList<String> result1 = new ArrayList<String>();
	for(int  i = 0 ;i < this.PageNumber ;i++) {
		Page p = deSerialize(this.Name+i+".ser");
		for(int j = 0 ; j < p.Tuples.size();j++) {
			result.add((Hashtable)p.Tuples.get(j));
		}
	}
	for(int i= 0;i<result.size();i++) {
		if(result.get(i).get(v.keySet().toArray()[0]).equals(v.get(v.keySet().toArray()[0]))) {
			result.remove(i);
			i--;
		}
	}
	for(int i = 0;i<result.size();i++) {
		result1.add(result.get(i).toString());
	}
	return result1;
}
public ArrayList sqlSearchEquals(Hashtable v) throws DBAppException {
	ArrayList<String> result = new ArrayList<String>();
	if(this.GridNumber==0) {
		for(int i =0;i< this.PageNumber;i++) {
			Page p = deSerialize(this.Name+i+".ser");
			return p.sqlSearch(v);
		}
	}
	else {
		GridIndex g =null;
		Boolean flag = false;
		for(int i = 0 ;i < this.GridNumber ;i++) {
			g = deSerializeGrid("G"+this.Name+i);
			for(int j = 0;j<g.Columns.length;j++) {
				if(v.keySet().toArray()[0].equals(g.Columns[j])) {
					flag = true;
					break;
				}
			}
			if(flag) {
				break;
			}
		}
		if(flag) {
			
		ArrayList<Hashtable> result1 = new ArrayList<Hashtable>();
		ArrayList<String> y = g.sqlSearchEquals(v);
		String number = "";
		for(int i = 0 ;i< y.size();i++) {
			
			Bucket b = deSerializeBucket(g.Name+"B"+y.get(i));
			for(int j =0;j< b.pointers.size();j++) {
				String[] c = b.pointers.get(j).split(",");
				Page p = deSerialize(c[0]);
				int u = Integer.parseInt(c[1]);
				result1.add(p.get(u));
			}
		}
		System.out.println(result1);
		for(int i=0;i<result1.size();i++) {
			if(compareTo((result1.get(i).get(v.keySet().toArray()[0])),(v.get(v.keySet().toArray()[0]))) == 0) {
				System.out.println("true");
				result.add(result1.get(i).toString());
			}
		}
	}
		else {
			for(int i =0;i< this.PageNumber;i++) {
				Page p = deSerialize(this.Name+i+".ser");
				return p.sqlSearch(v);
			}
		}
	}
	return result;
}

public int BSearch(Object key,int FirstPage,int LastPage) {
	int Half = 0;
	while(FirstPage < LastPage) {
	Half = (((LastPage+FirstPage))/2);
	Page p = deSerialize(this.Name+Half+".ser");
	if(compareTo(key,p.min) > 0) {
		if(compareTo(key,p.max) < 0) {
			
			return Half;
			}
		else {
			FirstPage = Half+1;
		}
		}
	else {
		LastPage = Half-1;
	}
	}
	if(FirstPage == LastPage) {
		Page p = deSerialize(this.Name+FirstPage+".ser");
		if(compareTo(key,p.min) > 0) {
			if(compareTo(key,p.max) < 0) {
				return FirstPage;
				}
			else {
				return -1;
			}
		}
		else {
			return -1;
		}
	}
	else {
		return -1;
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
public static Bucket deSerializeBucket(String fileName) {
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
public static GridIndex deSerializeGrid(String fileName) {
	try {
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\Grids\\"+ fileName;
		
        FileInputStream fileIn = new FileInputStream(abFilePath);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        GridIndex e = (GridIndex) in.readObject();
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
public void Update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue,String type) throws DBAppException, ParseException {
	if(this.GridNumber ==0) {
	if(this.PageNumber== 0) {
		throw new DBAppException("This table has no tuples");
	}
	else {
		Object key=null;
		switch(type) {
		case"java.lang.String":key = clusteringKeyValue; break;
		case"java.lang.Integer":key = Integer.parseInt(clusteringKeyValue);break;
		case"java.lang.Double":key = Double.parseDouble(clusteringKeyValue);break;
		case"java.util.Date": SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");key = dateformat.parse(clusteringKeyValue);break;
		default:break;
		}
		int pagenum = this.BSearch(key, 0, this.PageNumber-1);
		if(pagenum == -1) {
			throw new DBAppException("This ID doesn't exist in this table");
		}
		else {
			Page P = deSerialize(this.Name+pagenum+".ser");
			P.update(clusteringKeyValue,columnNameValue);
			P.Serialize();
	}
	
	}
	}
	else {
		boolean flag = false;
		GridIndex g = null;
		for(int  i =0 ;i<this.GridNumber;i++) {
			g = deSerializeGrid("G"+this.Name+i);
			for(int j = 0 ; j < g.Columns.length;j++) {
				if(g.Columns[i].equals(this.Key)) {
					flag = true;
					break;
				}
			}
			if(flag) {
				break;
			}
		}
		if(!flag) {
			if(this.PageNumber== 0) {
				throw new DBAppException("This table has no tuples");
			}
			else {
				Object key=null;
				switch(type) {
				case"java.lang.String":key = clusteringKeyValue; break;
				case"java.lang.Integer":key = Integer.parseInt(clusteringKeyValue);break;
				case"java.lang.Double":key = Double.parseDouble(clusteringKeyValue);break;
				case"java.util.Date": SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");key = dateformat.parse(clusteringKeyValue);break;
				default:break;
				}
				int pagenum = this.BSearch(key, 0, this.PageNumber-1);
				if(pagenum == -1) {
					throw new DBAppException("This ID doesn't exist in this table");
				}
				else {
					Page P = deSerialize(this.Name+pagenum+".ser");
					int row = P.BSearchInPage(key, 0, P.Tuples.size()-1);
					Hashtable bilo = P.get(row);
					P.update(clusteringKeyValue,columnNameValue);
					Hashtable biloUpdated = P.get(row);
					P.Serialize();
					for(int i = 0 ;i < this.GridNumber;i++ ) {
						g = deSerializeGrid("G"+this.Name+i);
						g.removePointer(bilo);
						g.Insert(biloUpdated, P.Path, row);
						g.Serialize();
					}
					return;
			}
			
			}
		}
		else {
			Object key=null;
			switch(type) {
			case"java.lang.String":key = clusteringKeyValue; break;
			case"java.lang.Integer":key = Integer.parseInt(clusteringKeyValue);break;
			case"java.lang.Double":key = Double.parseDouble(clusteringKeyValue);break;
			case"java.util.Date": SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");key = dateformat.parse(clusteringKeyValue);break;
			default:break;
			}
			Hashtable v = new Hashtable();
			v.put(this.Key,key);
			ArrayList<String> results = g.sqlSearchEquals(v);
			ArrayList<Hashtable> result1 = new ArrayList<Hashtable>();
			Boolean flag1 = false;
			Page p =null;
			int u =0;
			int y = 0;
			Hashtable bilo =null;
			for(int i = 0 ;i< results.size();i++) {
				Bucket b = deSerializeBucket(g.Name+"B"+results.get(i));
				for(int j =0;j< b.pointers.size();j++) {
					String[] c = b.pointers.get(j).split(",");
					p = deSerialize(c[0]);
					u = Integer.parseInt(c[1]);
					bilo = p.get(u);
					if(compareTo(bilo.get(this.Key),key)==0) {
						flag1 = true;
						y =j;
						break;
					}
				}
				if(flag1) {
					break;
				}
			}
			p.update(clusteringKeyValue, columnNameValue);
			Hashtable biloUpdated = p.get(u);
			p.Serialize();
			for(int i = 0 ;i < this.GridNumber;i++ ) {
				g = deSerializeGrid("G"+this.Name+i);
				g.removePointer(bilo);
				g.Insert(biloUpdated, p.Path, u);
				g.Serialize();
			}
			return;
			
		}
	}
}


public void addIndex(String[] columnNames, ArrayList<String> min, ArrayList<String> max, ArrayList<String> type) throws DBAppException {
	GridIndex og = new GridIndex(this.Name,this.GridNumber,columnNames,min,max,type,this) ;
	this.GridNumber++;
	og.Serialize();
}

public static void main(String[]args) {
}

}
