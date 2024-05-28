import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;


public class DBApp {
static Vector Tables = new Vector<Table>();
	
	public void init() {
		
	}
	public static int compareTo(Object a,String b,String columnType) throws ParseException {
		if(columnType.equals("java.lang.String")) {
			String a1 = (String) a;
			String b1 = (String) b;
			return a1.compareTo(b1);
		}
		if(columnType.equals("java.lang.Integer")) {
			Integer a2 =(Integer)(a);
			Integer b2 =Integer.parseInt(b);
			return a2.compareTo(b2);
		}
		if(columnType.equals("java.lang.Double")) {
			Double a3 = (Double)(a);
			Double b3 = Double.parseDouble(b);
		}
		if(columnType.equals("java.util.Date")) {
			SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
			Date a1 = (Date) a;
			String aDateString = dateformat.format(a1);
			Date bDate = dateformat.parse(b);
			String bDateString = dateformat.format(bDate);
			return (aDateString.compareTo(bDateString));
		}
		else {
			return 0;
		}
	}
	public static boolean checkType(String type) {
		if(type.equals("java.lang.String")) {
			return true;
		}
		if(type.equals("java.lang.Integer")) {
			return true;
		}
		if(type.equals("java.lang.Double")) {
			return true;
		}
		if(type.equals("java.util.Date")) {
			return true;
		}
		else {
			return false;
		}
	}
	

	
	public static void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		Table T = new Table(tableName,clusteringKey);
		DBApp.Tables.add(T);
		try {
			String filename = "metadata.csv";
			String workingDirectory = System.getProperty("user.dir");
			String abFilePath = "";
			abFilePath = workingDirectory + File.separator +"src\\main\\resources\\"+ filename;
			
			FileWriter CSVWriter = new FileWriter(abFilePath,true);
			for(int i=0;i< colNameType.size();i++) {
				
				
				if(checkType((String) colNameType.get(colNameType.keySet().toArray()[i]))) {
				CSVWriter.append(tableName);
				CSVWriter.append(",");
				CSVWriter.append((String) colNameType.keySet().toArray()[i]);
				CSVWriter.append(",");
				CSVWriter.append((String) colNameType.get(colNameType.keySet().toArray()[i]));
				CSVWriter.append(",");
				if(((String)colNameType.keySet().toArray()[i]).equals(clusteringKey)) {
					CSVWriter.append("TRUE");
				}
				else {
					CSVWriter.append("FALSE");
				}
				CSVWriter.append(",");
				CSVWriter.append("FALSE");
				CSVWriter.append(",");
				CSVWriter.append(String.valueOf(colNameMin.get(colNameMin.keySet().toArray()[i])));
				
				CSVWriter.append(",");
				CSVWriter.append(String.valueOf(colNameMax.get(colNameMax.keySet().toArray()[i])));
				CSVWriter.append("\n");
				}
			}
			CSVWriter.close();
		} catch (IOException e) {
			System.out.println("An ERROR has occured");
			e.printStackTrace();
			e.getCause();
		}
	}

	
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		ArrayList<String> min = new ArrayList<String>(); 
		ArrayList<String> max = new ArrayList<String>(); 
		ArrayList<String> type = new ArrayList<String>(); 
		try {
		String filename = "metadata.csv";
		String workingDirectory = System.getProperty("user.dir");
		String abFilePath = "";
		abFilePath = workingDirectory + File.separator +"src\\main\\resources\\"+ filename;
		BufferedReader br = new BufferedReader(new FileReader(abFilePath));
		ArrayList<String[]> AllLines=new ArrayList<String[]>();
		String current=br.readLine();
		while(current !=null) {
			AllLines.add(current.split(","));
			current = br.readLine();
		}
		br.close();
		ArrayList<Integer> indekies = new ArrayList<Integer>();
		for(int  i = 0 ; i < AllLines.size() ; i++ ) {
			if(((String[])AllLines.get(i))[0].equals(tableName)) {
				indekies.add(i);
			}
		}
		for(int i = 0 ; i < columnNames.length ; i++) {
			for(int j = 0 ; j < indekies.size() ; j++) {
			if(((String[])AllLines.get(indekies.get(j)))[1].equals(columnNames[i])) {
				min.add(((String[])AllLines.get(indekies.get(j)))[5]);
				max.add(((String[])AllLines.get(indekies.get(j)))[6]);
				type.add(((String[])AllLines.get(indekies.get(j)))[2]);
			}
			}
		}
			
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		Table T = null;
		boolean flag = false;
		for(int i = 0 ; i < this.Tables.size();i++) {
			if(((Table)Tables.get(i)).Name.equals(tableName)) {
				T = ((Table)Tables.get(i));
				flag = true;
				break;
			}
		}
		if(!flag) {
			throw new DBAppException("This table doesn't exist");
		}
		else {
			T.addIndex(columnNames,min,max,type);
		}
	}
	public static boolean checkViability(String key,Object value,String tableName) throws ParseException {
		try {
			String filename = "metadata.csv";
			String workingDirectory = System.getProperty("user.dir");
			String abFilePath = "";
			abFilePath = workingDirectory + File.separator +"src\\main\\resources\\"+ filename;
			
			BufferedReader br = new BufferedReader(new FileReader(abFilePath));
			ArrayList<String[]> AllLines=new ArrayList<String[]>();
			String current=br.readLine();
			while(current !=null) {
				AllLines.add(current.split(","));
				current = br.readLine();
			}
			br.close();
			boolean flag = false;
			for(int i = 0 ; i <AllLines.size();i++) {
				if(((String[])AllLines.get(i))[0].equals(tableName)) {
					if(((String[])AllLines.get(i))[1].equals(key)) {
						String type = ((String[])AllLines.get(i))[2];
						String s = value.getClass().toString();
						String[] d= s.split(" ");
						s = d[1];
						if(s.equals(type)) {
							String min = ((String[])AllLines.get(i))[5];
							String max = ((String[])AllLines.get(i))[6];
							if(compareTo(value,min,s) >= 0 && compareTo(value,max,s) <= 0) {							
								return true;
							}
						}
					}
				}
			}
			return false;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return false;
		
	}

	
	public void insertIntoTable(String tableName, Hashtable<String,Object> colNameValue) throws DBAppException, ParseException, IOException {
		Table T = null;
		boolean flag = false;
		for(int i = 0 ; i < this.Tables.size();i++) {
			if(((Table)Tables.get(i)).Name.equals(tableName)) {
				T = ((Table)Tables.get(i));
				flag = true;
				break;
			}
		}
		if(!flag) {
			throw new DBAppException("This table doesn't exist");
		}
		boolean canInsert = true;
		for(int i = 0;i <colNameValue.size();i++) {
			if (!checkViability(((String)colNameValue.keySet().toArray()[i]), colNameValue.get(colNameValue.keySet().toArray()[i]),tableName)) {
				canInsert = false;
				break;
			}
		}
			if(canInsert) {
				T.Insert(colNameValue);
			}
			else {
				throw new DBAppException("Something went wrong, check inputs");
			}
	}

	
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, ParseException {
		Table T = null;
		boolean flag = false;
		for(int i = 0 ; i < this.Tables.size();i++) {
			if(((Table)Tables.get(i)).Name.equals(tableName)) {
				T = ((Table)Tables.get(i));
				flag = true;
				break;
			}
		}
		if(!flag) {
			throw new DBAppException("Table " + tableName +" doesn't exist");
		}
		boolean canUpdate = true;
		for(int i = 0;i <columnNameValue.size();i++) {
			if (!checkViability(((String)columnNameValue.keySet().toArray()[i]), columnNameValue.get(columnNameValue.keySet().toArray()[i]),tableName)) {
				canUpdate = false;
				break;
			}
		}
		try {
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Omar\\Downloads\\DB2Project\\src\\main\\resources\\metadata.csv"));
			ArrayList<String[]> AllLines=new ArrayList<String[]>();
			String current=br.readLine();
			while(current !=null) {
				AllLines.add(current.split(","));
				current = br.readLine();
			}
			String type="";
			for(int i = 0;i<AllLines.size();i++) {
				if(((String[])AllLines.get(i))[0].equals(tableName) && ((String[])AllLines.get(i))[3].equals("TRUE")) {
					type = ((String[])AllLines.get(i))[2];
				}
			}
			
			if(canUpdate) {
				T.Update(clusteringKeyValue,columnNameValue,type);
			}
			else {
				throw new DBAppException("Something went wrong, check inputs");
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		Table T = null;
		boolean flag = false;
		for(int i = 0 ; i < this.Tables.size();i++) {
			if(((Table)Tables.get(i)).Name.equals(tableName)) {
				T = ((Table)Tables.get(i));
				flag = true;
				break;
			}
		}
		if(!flag) {
			throw new DBAppException("Table " + tableName +" doesn't exist");
		}
		else {
		T.Delete(columnNameValue);
		}
	}
		
		
	

	
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		
			String tableName = sqlTerms[0]._strTableName;
			Table T = null;
			boolean flag = false;
			for(int j = 0 ; j < this.Tables.size();j++) {
				if(((Table)Tables.get(j)).Name.equals(tableName)) {
					T = ((Table)Tables.get(j));
					flag = true;
					break;
				}
			}
			if(!flag) {
				throw new DBAppException("This table doesn't exist");
			}
			else {
				int k =0;
				ArrayList<String> firstTerm  = new ArrayList<String>();
				ArrayList<String> secondTerm = new ArrayList<String>();
				ArrayList<String> result = new ArrayList<String>();
				for(int i = 0 ; i< sqlTerms.length ; i++) {
					System.out.println(sqlTerms[i]._objValue+"   "+sqlTerms[i]._strColumnName);
					if(i==0) {
					firstTerm = Execute(sqlTerms[i],T);
					System.out.println(firstTerm+"F Term");
					}
					else {
						secondTerm = Execute(sqlTerms[i],T);
						System.out.println(secondTerm+"S Term");
						String op = arrayOperators[k];
						k++;
						switch(op) {
						case "AND":firstTerm = ArrayAND(firstTerm,secondTerm);break;
						case "OR" :firstTerm = ArrayOR(firstTerm,secondTerm);break;
						case "XOR":firstTerm = ArrayXOR(firstTerm,secondTerm);break;
						default:throw new DBAppException("Invalid operator");
						}
					}
					result = firstTerm;
				}
				System.out.println(result+"Final");
				return result.iterator();
			}
		
	}
	public static ArrayList Execute(SQLTerm Travvy,Table Betra) throws DBAppException {//Travvy & Betra are individuals with a very high IQ(>250)
		Hashtable Hazemovic = new Hashtable();
		Hazemovic.put(Travvy._strColumnName, Travvy._objValue);
		switch(Travvy._strOperator) {
		case "=" :return Betra.sqlSearchEquals(Hazemovic);
		case "!=":return Betra.sqlSearchNotEquals(Hazemovic);
		case ">" :return Betra.sqlSearchBigger(Hazemovic);
		case ">=":return Betra.sqlSearchBiggerEqual(Hazemovic);
		case "<" :return Betra.sqlSearchLess(Hazemovic);
		case "<=":return Betra.sqlSearchLessEqual(Hazemovic);
			default:throw new DBAppException("Invalid operator");
		}
	}
	public static ArrayList ArrayAND(ArrayList<String> a,ArrayList<String> b) {
		ArrayList<String> result = new ArrayList<String>();
		ArrayList<String> min = null;
		ArrayList<String> max = null;
		if(a.size()<b.size()) {
			min = a;max=b;
		}
		else {
			min = b;max = a;
		}
		for(int i = 0;i < min.size();i++) {
			if(max.contains(min.get(i))) {
				result.add(min.get(i));
			}
		}
		return result;
	}
	public static ArrayList ArrayOR(ArrayList<String> a,ArrayList<String> b) {
		
		ArrayList<String> min = null;
		ArrayList<String> max = null;
		if(a.size()<b.size()) {
			min = a;max=b;
		}
		else {
			min = b;max = a;
		}
		ArrayList<String> result = max;
		for(int i = 0 ; i < min.size(); i++) {
			if(!(max.contains(min.get(i)))) {
				result.add(min.get(i));
			}
		}
		return result;
	}
	public static ArrayList ArrayXOR(ArrayList<String> a,ArrayList<String> b) {
		ArrayList<String> resultOR = ArrayOR(a,b);
		ArrayList<String> resultAND = ArrayAND(a,b);
		for(int i = 0 ; i < resultOR.size();i++) {
			if(resultOR.contains(resultAND.get(i))) {
				resultOR.remove(resultAND.get(i));
				i--;
			}
		}
		return resultOR;
	}
	public static void main(String[]args) throws DBAppException {
	}


	
	
}
