import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author linxiaofeng
 * @date 2018/12/19
 */
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final long MAX_MEMORY = 4L * 1024 * 1024 * 1024;
        final int MAX_THREAD = 18;
        String input,output,directory;
        if(args.length != 3){
            input = "test20.txt";
            output = "result.txt";
            directory = "./work/";
        }
        else {
            input = args[0];
            output = args[1];
            directory = args[2];
        }
        long tStart = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        long end;

        File file = new File(input);
        FileInputStream fileInputStream = new FileInputStream(file);
        int length = fileInputStream.available();
        long startPos = 0,endPos = 0;
        String fileName;
        long blockSize = length / MAX_THREAD;
        List<String> fileNames = new ArrayList<>();
        ExecutorService executorService = new ThreadPoolExecutor(6,6,0L, TimeUnit.SECONDS,new LinkedBlockingQueue<>(16));
        List<ReadAndSortThread> threads = new ArrayList<>();
        for(int i = 0; i <= MAX_THREAD; i++){
            startPos = i == 0 ? 0 : endPos;
            endPos = getPosition(file,(i+1) * blockSize);
            fileName = directory + i + ".txt";
            fileNames.add(fileName);
            ReadAndSortThread readAndSortThread = new ReadAndSortThread(file,startPos,endPos,fileName);
            threads.add(readAndSortThread);
        }
        List<Future<String>> lists = executorService.invokeAll(threads);
        executorService.shutdown();
        end = System.currentTimeMillis();
        System.out.println("readsort:" + (end-start));
        start = System.currentTimeMillis();
        mergeSort(fileNames, output);
        end = System.currentTimeMillis();
        System.out.println("mergesort:" + (end-start));
        long tEnd = System.currentTimeMillis();
        System.out.println("all:" + (tEnd-tStart));
    }

    private static byte NEXT_LINE = "\n".getBytes()[0];
    private static int getNextLinePosition(byte[] bytes){
        int pos = -1;
        for(int i = 0; i < bytes.length; i++){
            if(NEXT_LINE == bytes[i]){
                pos = i;
                break;
            }
        }
        return pos;
    }
    private static int getNextLinePosition(byte[] bytes,int start){
        int pos = -1;
        for(int i = start; i < bytes.length; i++){
            if(NEXT_LINE == bytes[i]){
                pos = i;
                break;
            }
        }
        return pos;
    }
    private static long getPosition(File file, long pos) throws IOException {
        FileChannel fileChannel = new RandomAccessFile(file,"r").getChannel();
        fileChannel.position(pos);
        ByteBuffer byteBuffer = ByteBuffer.allocate(129);
        byte[] bytes;
        int size;
        if(fileChannel.read(byteBuffer) != -1){
            size = byteBuffer.position();
            bytes = new byte[size];
            byteBuffer.rewind();
            byteBuffer.get(bytes);
            byteBuffer.clear();
            pos += getNextLinePosition(bytes);
            return pos + 1;
        }
        else{
            return -1L;
        }
    }

    private static final int  BUF_SIZE = 64 * 1024;
    private static class ReadAndSortThread implements Callable<String>{
        File file;
        long start,end;
        String name;
        public ReadAndSortThread(File file, long start, long end, String name){
            this.file = file;
            this.start = start;
            this.end = end;
            this.name = name;
        }
        @Override
        public String call() throws Exception {
            try {
                long clockStart,clockEnd;
                clockStart = System.currentTimeMillis();
                FileChannel fileChannel = new RandomAccessFile(this.file,"r").getChannel();
                fileChannel.position(start);
                ByteBuffer byteBuffer = ByteBuffer.allocate(BUF_SIZE);
                List<String> lists = new ArrayList<>();
                byte[] bytes,left,all;
                int leftLength = 0;
                left = new byte[128];
                long cur = start;
                boolean isEnd =false;
                while(fileChannel.read(byteBuffer) != -1){
                    int size = byteBuffer.position();
                    cur += size;
                    if(end > 0 && cur > end){
                        size = (int)(size + end - cur);
                        isEnd = true;
                    }
                    bytes = new byte[size];
                    byteBuffer.rewind();
                    byteBuffer.get(bytes);
                    byteBuffer.clear();
                    if(leftLength != 0){
                        all = new byte[leftLength + size];
                        System.arraycopy(left,0,all,0,leftLength);
                        System.arraycopy(bytes,0,all,leftLength,size);
                    }
                    else{
                        all = bytes;
                    }
                    int curStart = 0;
                    int nextLine;
                    while((nextLine = getNextLinePosition(all,curStart)) != -1){
                        lists.add(new String(all,curStart, (nextLine - curStart)));
                        curStart = nextLine + 1;
                    }
                    if(curStart < all.length){
                        leftLength = all.length - curStart;
                        System.arraycopy(all,curStart,left,0,leftLength);
                    }
                    else{
                        leftLength = 0;
                    }
                    if(isEnd){
                        break;
                    }
                }
                clockEnd = System.currentTimeMillis();
                System.out.println(name + "read:" + (clockEnd - clockStart));
                clockStart = System.currentTimeMillis();
                Collections.sort(lists);
                clockEnd = System.currentTimeMillis();
                System.out.println(name + "sort:" + (clockEnd - clockStart));
                clockStart = System.currentTimeMillis();
                File tmpFile = new File(this.name);
                BufferedWriter bw=new BufferedWriter(new FileWriter(tmpFile));
                for(int i=0;i<lists.size();i++){
                    bw.write(lists.get(i)+ "\n");
                }
                bw.close();
                clockEnd = System.currentTimeMillis();
                System.out.println(name + "write:" + (clockEnd - clockStart));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return "";
        }

    }

    private static void mergeSort(List<String> files, String output) throws IOException {
        File resultFile=new File(output);
        BufferedWriter bw=new BufferedWriter(new FileWriter(resultFile));

        PriorityQueue<Node> priorityQueue = new PriorityQueue<>();
        for(int i = 0;i  < files.size(); i++){
            File file = new File(files.get(i));
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String line = bufferedReader.readLine();
            if (line != null){
                priorityQueue.add(new Node(bufferedReader,line));
            }
        }

        while(!priorityQueue.isEmpty()){
            Node cur = priorityQueue.poll();
            bw.write(cur.val + "\n");
            String next = cur.bf.readLine();
            if(next != null){
                cur.val = next;
                priorityQueue.offer(cur);
            }
        }
        bw.close();
    }

    static class Node implements Comparable<Node>{
        BufferedReader bf;
        String val;
        public Node(BufferedReader bf, String val){
            this.bf = bf;
            this.val = val;
        }

        @Override
        public int compareTo(Node o) {
            return this.val.compareTo(o.val);
        }
    }

    private static String quickSort(String[] lines, int capacity, String directory) throws IOException {
        Arrays.sort(lines,0,capacity);
        String fileName = directory + System.currentTimeMillis() + ".txt";
        File tmpFile = new File(fileName);
        BufferedWriter bw=new BufferedWriter(new FileWriter(tmpFile));
        for(int i=0;i<capacity;i++){
            bw.write(lines[i]+ "\n");
        }
        bw.close();
        return fileName;
    }
}
