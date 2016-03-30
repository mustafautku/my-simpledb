package testskyline;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import simpledb.metadata.MetadataMgr;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;
import static simpledb.file.Page.BLOCK_SIZE;

/**
/*
 * Generate int/double data. Here's the schema:
 *  
 * 	INPUT(id int, aint INT, bint INT, // random(independent) integers 
 * 			aind DOUBLE, bind DOUBLE, cind DOUBLE, dind DOUBLE,eind DOUBLE,  // independent doubles
 * 			acor DOUBLE, bcor DOUBLE,  // correlated doubles.
 * 			aunc DOUBLE, bunc DOUBLE,  // uncorrelated doubles
 * 			C varchar(8)) 
 * 
 * 
 * The java.util.Random class provides more flexible
 * ways to generate uniformly distributed random numbers, providing easy
 * generation of types other than double, as well as providing a Gaussian
 * distribution. // ORNEK:::generate random (0,1)UNIFORM // double
 * _A=Math.random(); // rf.setDouble("a", _A); // double _B=Math.random(); //
 * rf.setDouble("b", _B); //generate random (10,20) UNIFORM // Random _rgen1=
 * new Random(); // double _A=10+_rgen1.nextDouble()*10; // rf.setDouble("a",
 * _A); // // Random _rgen2= new Random(); // double
 * _B=10+_rgen2.nextDouble()*10; // rf.setDouble("b", _B);
 * 
 * @author mustafautku
 * 
 */
public class InitData {
	public static int numberOfTuples = 100;

	public static void initData(String dbdir) {
		System.out.println("BEGIN INITIALIZATION");
		SimpleDB.init(dbdir);
		if (SimpleDB.fileMgr().isNew()) {
			System.out.println("loading data");
			MetadataMgr md = SimpleDB.mdMgr();
			Transaction tx = new Transaction();

			// create and populate  table containing the INT and DOUBLE values
	         Schema sch = new Schema();
	         sch.addIntField("id"); // 4B
	         sch.addIntField("aint");  // 4B
	         sch.addIntField("bint");  // 4B
	         
	         sch.addDoubleField("aind");  // 8B
	         sch.addDoubleField("bind");  // 8B	         
	         sch.addDoubleField("cind");  // 8B
	         sch.addDoubleField("dind");  // 8B
	         sch.addDoubleField("eind");  // 8B
	         
	         sch.addDoubleField("acor"); // 8B
			 sch.addDoubleField("bcor"); // 8B
			 
			 sch.addDoubleField("aunc"); // 8B
			 sch.addDoubleField("bunc"); // 8B 
			 sch.addStringField("str",8); // 12B string
	         // total record size = 96B, slot size=100
	        
	         md.createTable("input", sch, tx);
	         TableInfo ti = md.getTableInfo("input", tx);
	         
	         RecordFile rf = new RecordFile(ti, tx);
	         while (rf.next())
	            rf.delete();
	         rf.beforeFirst();
	         
//			// create and populate the student table
//			sch = new Schema();
//			sch.addIntField("id"); // 4B
//			sch.addDoubleField("a"); // 8B
//			sch.addDoubleField("b"); // 8B
//			sch.addStringField("c", 80); // 84B
//			md.createTable("inputI", sch, tx); // record size:100B ( slot size=104)
//			TableInfo tiI = md.getTableInfo("inputI", tx);
//			RecordFile rfI = new RecordFile(tiI, tx);
//			while (rfI.next())
//				rfI.delete();
//			rfI.beforeFirst();
//
//			md.createTable("inputC", sch, tx); // record size: 100B
//			TableInfo tiC = md.getTableInfo("inputC", tx);
//			RecordFile rfC = new RecordFile(tiC, tx);
//			while (rfC.next())
//				rfC.delete();
//			rfC.beforeFirst();
//
//			md.createTable("inputU", sch, tx); // record size: 100B
//			TableInfo tiU = md.getTableInfo("inputU", tx);
//			RecordFile rfU = new RecordFile(tiU, tx);
//			while (rfU.next())
//				rfU.delete();
//			rfU.beforeFirst();

			// for (int id=0; id<numberOfTuples; id++) {
			int counter = 0;
			Random _rgen0= new Random();
			Random _rgen1= new Random();
			Random _rgen2 = new Random();
			Random _rgen3 = new Random();
			Random _rgen4 = new Random();

			while (counter < numberOfTuples) {		
	            
				// int data
	            int _A=_rgen0.nextInt(numberOfTuples);	            
	            int _B=_rgen1.nextInt(numberOfTuples);           
	            
				// correlated data
				double sqrtOf2 = Math.sqrt(2.0);
				double var = sqrtOf2 / 8;
				double Q = 0.0;
				double planeOnLine1 = Q + _rgen0.nextGaussian() * var;
				if (Math.abs(planeOnLine1) > sqrtOf2 / 2)
					continue;
				double planeXcor = 0.5 + planeOnLine1 / sqrtOf2;
				double planeYcor = 0.5 + planeOnLine1 / sqrtOf2;
				double planeXuncor = 0.5 + planeOnLine1 / sqrtOf2;
				double planeYuncor = 0.5 - planeOnLine1 / sqrtOf2;
				// var=var-Math.abs(planeOnLine1);//var=((var-Math.abs(planeOnLine1))/var
				// ) *var;
				double planeOnLine2 = Q + _rgen1.nextGaussian() * var / 2;
				
				//generate correlated & uncorrelated data
				double _Acor = planeXcor + planeOnLine2 / sqrtOf2;
				double _Bcor = planeYcor - planeOnLine2 / sqrtOf2;
				double _Auncor = planeXuncor + planeOnLine2 / sqrtOf2;
				double _Buncor = planeYuncor + planeOnLine2 / sqrtOf2;
				// generate random (0,1)UNIFORM
				double _Aind = _rgen0.nextDouble();
				double _Bind = _rgen1.nextDouble();
				double _Cind = _rgen2.nextDouble();
				double _Dind = _rgen3.nextDouble();
				double _Eind = _rgen4.nextDouble();
				
				if ((0.1 < _Acor && _Acor < 1.0)&& (0.1 < _Bcor && _Bcor < 1.0) // uncor'e bakmay gerek yok. Bu geciyorsa, o da gecer
						&& (0.1 < _Aind && _Aind < 1.0) && (0.1 < _Bind && _Bind < 1.0)
						&& (0.1 < _Cind && _Cind < 1.0) && (0.1 < _Dind && _Dind < 1.0)
						&& (0.1 < _Eind && _Eind < 1.0) ) 
					counter++;
				else
					continue;
				
				rf.insert();
//				rfI.insert();
//				rfC.insert();
//				rfU.insert();
//				
				
				rf.setInt("id", counter);
				rf.setInt("aint", _A);
				rf.setInt("bint", _B);

				rf.setDouble("aind", _Aind);
				rf.setDouble("bind", _Bind);
				rf.setDouble("cind", _Cind);
				rf.setDouble("dind", _Dind);
				rf.setDouble("eind", _Eind);
				
				rf.setDouble("acor", _Acor);
				rf.setDouble("bcor", _Bcor);
				
				rf.setDouble("aunc", _Auncor);
				rf.setDouble("bunc", _Buncor);
				
				rf.setString("str", "dummy");			
				
//				rfI.setDouble("a", _Ai);
//				rfI.setDouble("b", _Bi);
//				rfI.setString("c", "dummy");
//				
//				rfC.setDouble("a", _Acor);
//				rfC.setDouble("b", _Bcor);
//				rfC.setString("c", "dummy");
//				
//				rfU.setDouble("a", _Auncor);
//				rfU.setDouble("b", _Buncor);
//				rfU.setString("c", "dummy");				
//				
			}
			
			rf.close();
//			rfI.close();
//			rfC.close();
//			rfU.close();

			tx.commit();
			tx = new Transaction();
			tx.recover(); // add a checkpoint record, to limit rollback
		}
	}
	public static ArrayList<Double> getAllPoints(List<String> skyfields, Transaction tx){
		ArrayList<Double> AllPoints=new ArrayList<Double>();
		MetadataMgr md = SimpleDB.mdMgr();
		TableInfo ti = md.getTableInfo("input", tx);
		RecordFile rf = new RecordFile(ti, tx);
		while (rf.next()){
			AllPoints.add(rf.getDouble(skyfields.get(0)));
			AllPoints.add(rf.getDouble(skyfields.get(1)));
		}
		rf.close();
		return AllPoints;
	}
	public static void main(String[] args) {
		InitData.initData("skyline100"); 
		
		Transaction tx = new Transaction();
		MetadataMgr md = SimpleDB.mdMgr();
		TableInfo ti = md.getTableInfo("input", tx);
		Schema sch = ti.schema();
		
		RecordFile rf=new RecordFile(ti, tx);
		while(rf.next())
			System.out.println(rf.getInt("id")+ " "+rf.getInt("aint")+ " "+rf.getInt("bint")+ " "+
							   rf.getDouble("aind")+ " "+rf.getDouble("bind")+ " "+rf.getDouble("cind")+ " "+rf.getDouble("dind")+ " "+rf.getDouble("eind")+ " "+
							   rf.getDouble("acor")+ " "+rf.getDouble("bcor")+ " "+
							   rf.getDouble("aunc")+ " "+rf.getDouble("bunc"));
	}
}