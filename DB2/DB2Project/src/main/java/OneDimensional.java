import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.*;
public class OneDimensional implements java.io.Serializable{
ArrayList<String> ranges = new ArrayList<String>();
String Type ="";
String columnName = "";
Table mainTable = null;
public OneDimensional(String Type,String min,String max,String cName,Table T) {
	this.Split(Type, min, max);
	this.Type = Type;
	this.columnName = cName;
	this.mainTable = T;
}
public String createPointers(Hashtable tuple , String bucket) throws DBAppException {
	Object o =tuple.get(columnName);
	if(!(o instanceof Date)) {
	String x = o+"";
	for(int k = 0 ; k < ranges.size();k++) {
		String[] minmax = ranges.get(k).split(",");
		if(x.compareTo(minmax[0]) >= 0 && x.compareTo(minmax[1]) <= 0) {
			bucket = bucket+k;
			return bucket;
		}

}
	return "not found";
	}
	else {
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		Date o1 = (Date) o;
		String oDateString = dateformat.format(o1);
		for(int k = 0 ; k < ranges.size();k++) {
			String[] minmax = ranges.get(k).split(",");
			if(oDateString.compareTo(minmax[0]) >= 0 && oDateString.compareTo(minmax[1]) <= 0) {
				bucket = bucket+k;
				return bucket;
			}
	}
		return "not found";
	}

}
public void Split(String Type, String min , String max) {
	int[] rangeSize = new int[10];
	if(Type.equals("java.lang.String")) {
		String a = min;
		String b = max;
		if( 48 <= a.charAt(0) && a.charAt(0) <= 57 ) {//INTEGER
			int x = Integer.parseInt(a.substring(0, 2));
			int y = Integer.parseInt(b.substring(0, 2));
			int size = (y-x)+1;
			int mod10 = size%10;
			int divide = size/10;
			for(int i = 0 ; i< 10;i++) {
				if(i < mod10) {
					rangeSize[i] = divide ;
				}
				else {
					rangeSize[i] = divide-1;
				}
			}
			String begin = a;
			int z = x + rangeSize[0];
			String end = z+(b.substring(2));
			String e = begin + "," + end;
			this.ranges.add(e);
			for(int i = 1 ; i < 10 ; i++) {
				z++;
				begin = z + (a.substring(2));
				z = z+ rangeSize[i];
				end = z + (b.substring(2));
				e = begin + "," + end;
				this.ranges.add(e);
			}
		}
		else {//STRING
			int x = a.charAt(0);
			int y = b.charAt(0);
			int size = (y-x)+1;
			int mod10 = size%10;
			int divide = size/10;
			for(int i = 0 ; i< 10;i++) {
				if(i < mod10) {
					rangeSize[i] = divide ;
				}
				else {
					rangeSize[i] = divide-1;
				}
			}
			String begin = a;
			int z = x + rangeSize[0];
			char joe = (char) z;		
			String end = joe + (b.substring(1));
			String e = begin + "," + end;
			this.ranges.add(e);
			for(int i = 1 ; i < 10 ; i++) {
				z++;
				joe = (char)z;
				begin = joe + a.substring(1);
				z = z+rangeSize[i];
				joe =(char) z;
				end= joe+ b.substring(1);
				e = begin + "," + end;
				this.ranges.add(e);
			}
		}
	}
	if(Type.equals("java.lang.Integer")) {
		int a = Integer.parseInt(min);
		int b = Integer.parseInt(max);
		int size = (b-a)+1;
		int mod10 = size%10;
		int divide = size/10;
		for(int i = 0 ; i< 10;i++) {
			if(i < mod10) {
				rangeSize[i] = divide ;
			}
			else {
				rangeSize[i] = divide-1;
			}
		}
		int x = a;
		int y = a+rangeSize[0];
		String e = x+","+y;
		this.ranges.add(e);
		for(int i = 1 ; i < 10 ; i++) {
			x = y + 1;
			y = x + rangeSize[i];		
			e = x+","+y;
			this.ranges.add(e);
		}
	}
	if(Type.equals("java.lang.Double")) {
		Double a = Double.parseDouble(min);
		Double b = Double.parseDouble(max);
		Double size = (b-a)+0.10;
		Double mod10 = (size%1.00)*10;
		int divide = (int)(size/1);
		for(int i = 0 ; i< 10;i++) {
			if(i < mod10) {
				rangeSize[i] = divide;
			}
			else {
				rangeSize[i] = divide-1;
			}
		}
		DecimalFormat dec = new DecimalFormat("#0.00");
		Double x = a -0.01;
		Double y = a + (Double)(rangeSize[0]/10.00);
		String e = dec.format(x)+","+dec.format(y);
		this.ranges.add(e);
		for(int i = 1 ; i < 10 ; i++) {
			x = y + 0.10;
			y = x + (Double)(rangeSize[i]/10.00);
			x-=0.09;
			e = dec.format(x)+","+dec.format(y);
			this.ranges.add(e);
		}
	}
	if(Type.equals("java.util.Date")) {
		LocalDate a =LocalDate.parse(min);
		LocalDate b =LocalDate.parse(max);
		long days = ChronoUnit.DAYS.between(a, b) + 1;
		int mod10 = (int)(days%10);
		int divide = (int)(days/10);
		for(int i = 0 ; i< 10;i++) {
			if(i < mod10) {
				rangeSize[i] = divide ;
			}
			else {
				rangeSize[i] = divide-1;
			}
		}
		LocalDate x = a;
		LocalDate y = a.plusDays(rangeSize[0]);
		String e = x+","+y;
		this.ranges.add(e);
		for(int i = 1; i < 10; i++) {
			x = y.plusDays(1);
			y = x.plusDays(rangeSize[i]);
			e=x+","+y;
			this.ranges.add(e);
		}
		
	}
	else {
	}
}
public static void main(String[]args) {
}
}
