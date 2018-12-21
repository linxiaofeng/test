import java.io.*;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * @author linxiaofeng
 * @date 2018/12/19
 */
public class Main {
    public static void main(String[] args) throws IOException {
        if(args.length != 3){
            System.out.println("<input file name> <output file name> <working directory>");
            System.exit(1);
        }
        long start = System.currentTimeMillis();
        long end;
        String input = args[0];
        String output = args[1];
        String directory = args[2];
        File file = new File(input);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        final int SIZE = 10000000;
        String[] lines = new String[SIZE];
        String line;
        List<String> fileNames = new ArrayList<>();
        int index = 0;
        String fileName;
        while(true){
            line = bufferedReader.readLine();
            if(line == null){
                end = System.currentTimeMillis();
                System.out.println("read:" + (end-start));
                start = System.currentTimeMillis();
                fileName = quickSort(lines,index,directory);
                end = System.currentTimeMillis();
                System.out.println("quicksort:" + (end-start));
                start = System.currentTimeMillis();
                fileNames.add(fileName);
                break;
            }
            lines[index++] = line;
            if(index == SIZE){
                end = System.currentTimeMillis();
                System.out.println("read:" + (end-start));
                start = System.currentTimeMillis();
                fileName = quickSort(lines, index,directory);
                end = System.currentTimeMillis();
                System.out.println("quicksort:" + (end-start));
                start = System.currentTimeMillis();
                fileNames.add(fileName);
                index = 0;
            }
        }
        mergeSort(fileNames, output);
        end = System.currentTimeMillis();
        System.out.println("mergesort:" + (end-start));

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
