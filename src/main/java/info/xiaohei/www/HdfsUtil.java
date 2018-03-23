package info.xiaohei.www;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by xiaohei on 16/3/9.
 * HDFS操作类
 */
public class HdfsUtil {

    private static final String HDFS = "hdfs://192.168.0.130:9000/";
    private static final Configuration conf = new Configuration();
    private static FileSystem fs=null;

   
    static {
    	try {
			fs=FileSystem.get(URI.create(HDFS), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    /**
     * 创建文件夹
     *
     * @param folder 文件夹名
     */
    public static void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }

    /**
     * 删除文件夹,包括下面的内容
     *
     * @param folder 文件夹名
     */
    public static void deleteFile(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    /**
     * 重命名文件
     * @param src 源文件名
     * @param dst 目标文件名
     * */
    public static void rename(String src, String dst) throws IOException {
        Path name1 = new Path(src);
        Path name2 = new Path(dst);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        fs.rename(name1, name2);
        System.out.println("Rename: from " + src + " to " + dst);
        fs.close();
    }

    /**
     * 列出该路径的文件信息
     *
     * @param folder 文件夹名
     */
    public static void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    /**
     * 创建文件
     *
     * @param file    文件名
     * @param content 文件内容
     */
    public static void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }

    /**
     * 复制本地文件到hdfs
     *
     * @param local  本地文件路径
     * @param remote hdfs目标路径
     */
    public static void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    /**
     * 从hdfs下载文件到本地
     *
     * @param remote hdfs文件路径
     * @param local  本地目标路径
     */
    public static void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    /**
     * 查看hdfs文件内容
     *
     * @param remoteFile hdfs文件路径
     */
    public static void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }
    
    /**
     * 本地文件上传至 HDFS
     *
     * @param srcFile 源文件 路径
     * @param destPath hdfs路径
     */
    public static void uploadFileToHDFS(String srcFile,String destPath)throws Exception{

        FileInputStream fis=new FileInputStream(new File(srcFile));//读取本地文件
        Configuration config=new Configuration();
        FileSystem fs=FileSystem.get(URI.create(HDFS+destPath), config);
        OutputStream os=fs.create(new Path(destPath));
        //copy
        IOUtils.copyBytes(fis, os, 4096, true);
        System.out.println("拷贝完成...");
        fs.close();
    }
    
    
    /**
     * 上传文件或文件夹(不包含源根目录)
     * @param src
     * @param dest
     * @return
     * @throws Exception
     */
    public static boolean uploadFile(String src,String dest) throws Exception{  
    	File file = new File(src);  
    	if(!file.exists()) {
    		 throw new FileNotFoundException(src);
    	}
    	if(file.isDirectory()) {
    		if(!fs.exists(new Path(dest))){  
    			fs.mkdirs(new Path(dest));  
    		}
    		File[] files = file.listFiles();  
            for(int i = 0 ;i< files.length; i ++){  
                File f = files[i];  
                if(f.isDirectory()){  
                	uploadFile(f.getPath(),dest+File.separator+f.getName());  
                }else{  
                	 fs.copyFromLocalFile(new Path(f.getPath()), new Path(dest+File.separator+f.getName()));
            	     System.out.println("copy  file  from: " + f.getPath() + " to " + dest+File.separator+f.getName());
                }  
            }  
    	}else {
    		 fs.copyFromLocalFile(new Path(file.getPath()), new Path(dest+File.separator+file.getName()));
    	     System.out.println("copy  file  from: " + file.getPath() + " to " + dest+File.separator+file.getName());
    	}
        return true;  
    }  
    
    public static void main(String[] args) throws Exception {
    	HdfsUtil.uploadFile("C:\\Users\\Administrator\\Desktop\\data-cankao","input");
    }
}
