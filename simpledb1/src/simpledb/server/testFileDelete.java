package simpledb.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public class testFileDelete {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String homedir = System.getProperty("user.home");
	     File aDir = new File(homedir, "ornekDir");
	     
	     aDir.mkdir();
	     
	     RandomAccessFile f=null;
	     File aFile=new File(aDir,"ornekFile");
	     try {
			 f = new RandomAccessFile(aFile, "rws");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	     File[] filelist=aDir.listFiles();
	     System.out.println(filelist.length + " file exist");
//	     try {
//			f.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	     FileChannel fc=f.getChannel();
	     try {
			fc.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	     aFile.delete();  // We have to close before intending to delete
	     filelist=aDir.listFiles();
	     System.out.println(filelist.length + " file exist");
//	     System.out.println("file delete is " + success);
	}

}
