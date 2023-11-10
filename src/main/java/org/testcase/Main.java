
package org.testcase;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


public class Main {
    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        String inputFilePath = "src/main/resources/lng-4.txt.gz";
        HashSet<String> groups = new HashSet<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(new File(inputFilePath).toPath()))))) {
            String line;
            Pattern pattern = Pattern.compile("^(\"?\\w*\"?;)*(\"?\\w*\"?)+$");
            int i = 0;
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
//                    List<String> elements = Arrays.asList(line.split(";"));
                    groups.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(groups.size());

//        // Sort the groups by size in descending order
        List<List<String>> sortedGroups = new ArrayList<>();
        for (String string : groups) {
            List<String> line = Arrays.asList(string.split(";"));
            sortedGroups.add(line);
        }
        sortedGroups.sort(Comparator.comparingInt(List::size));
        int numThreads = 2; // Количество потоков для параллельной обработки
//        
        List<List<List<String>>> groupedLists = new ArrayList<>(groupStrings(sortedGroups, numThreads));
//           
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
//
            int i = 1;
            for (List<List<String>> group : groupedLists) {
                writer.write("Группа " + i++ + "\n");
//                        writer.write(addedLine.toString() + "\n");
                writer.write(group + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(groupedLists.size());
        time = System.currentTimeMillis() - time;
        System.out.printf("Elapsed %,9.3f ms\n", time / 1_000.0);
    }

    private static List<List<List<String>>> groupStrings(List<List<String>> checkedList, int numThreads) {
        List<List<List<String>>> groupedLists = new ArrayList<>();

        boolean[] checked = new boolean[checkedList.size()];
        int currentRowNumber = checkedList.size() - 1;
        ExecutorService executor = Executors.newFixedThreadPool(checkedList.get(currentRowNumber - 1).size());
        while (currentRowNumber >= 0) {

            List<String> currentRow = checkedList.get(currentRowNumber);
            List<List<String>> group = new ArrayList<>();
            group.add(currentRow);

            checked[currentRowNumber] = true;

            int refRowPos = 0;
            while (refRowPos < group.size()) {
                int refElemPos = 0;
                String elementToCompare = group.get(refRowPos).get(refElemPos);
                if (elementToCompare != null && !elementToCompare.equals("\"\"")) {
                    List<Integer> result = new ArrayList<>();
                    for (int refElemPosInner = 0; refElemPosInner < group.get(refRowPos).size(); refElemPosInner++) {
                        final int position = refElemPosInner;
                        executor.execute(() -> {
                            try {
                                for (int j = 0; j < checkedList.size() - 1; j++) {

                                    if (checkedList.get(j).size() > position
                                            && !checked[j]
                                            && checkedList.get(j).get(position).equals(elementToCompare)) {
                                        result.add(j);
                                    }
                                }

                            synchronized (checked) {
                                for (int pos : result) {
                                    checked[pos] = true;
                                    group.add(checkedList.get(pos));
                                }

                            }
                        } catch(Exception e){
                            e.printStackTrace();
                        }
                    });
                }
            }
//                    executor.shutdown();
            refRowPos++;

        }

        groupedLists.add(group);
            if (group.size()>1) {
                System.out.println(groupedLists.lastIndexOf(group));
            }
        currentRowNumber--;
//        if (groupedLists.size() % 100 == 0) {
//            System.out.println(groupedLists.size());
//        }
//        if (currentRowNumber % 10000 == 0) {
//            System.out.println(currentRowNumber);
//        }
    }

        return groupedLists;
}

}

//
//    private static List<Integer> findElement(int refRowPos, int refElemPos, String elementToCompare,
//                                             List<List<String>> group, List<List<String>> checkedList, boolean[] checked,
//                                             int start, int finish) {
//        List<Integer> result = new ArrayList<>();
//        while (refElemPos < group.get(refRowPos).size()) {
//            if (checkedList.get(start).size() < refElemPos) {
//                break;
//            }
//            for (int j = start; j < finish - 1; j++) {
//
//                if (checkedList.get(j).size() > refElemPos
//                        && !checked[j]
//                        && checkedList.get(j).get(refElemPos).equals(elementToCompare)) {
//                    result.add(j);
//                }
//            }
//
//            refElemPos++;
//        }
////        System.out.println("Thread finish " + refElemPos*refRowPos);
//        return result;
//    }
//}