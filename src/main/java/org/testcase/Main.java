
package org.testcase;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


public class Main {
    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        String inputFilePath = "src/main/resources/lng-4.txt.gz";
        HashSet<String> groups = new HashSet<>();
        HashMap<String, Integer> repeatedElements = new HashMap<>();
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
            for (String element : line) {
                if (repeatedElements.containsKey(element)) {
                    int newValue = repeatedElements.get(element) + 1;
                    repeatedElements.put(element, newValue);
                } else {
                    repeatedElements.put(element,1);
                }
            }
            sortedGroups.add(line);
        }
        List<String> repEl = repeatedElements.entrySet()
                .stream()
                .filter(x->x.getValue()>1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        long conttime = System.currentTimeMillis() - time;
        System.out.printf("Elapsed %,9.3f ms\n", conttime / 1_000.0);
        sortedGroups.sort(Comparator.comparingInt(List::size));
        Collections.reverse(sortedGroups);
        int numThreads = 8; // Количество потоков для параллельной обработки
//        
        List<List<List<String>>> groupedLists = new ArrayList<>(groupStrings(sortedGroups, numThreads, repEl, time));
        groupedLists.sort(Comparator.comparingInt(row-> row.size()));
//           Collections.reverse(groupedLists);
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
        System.out.printf("Finished %,9.3f ms\n", time / 1_000.0);
    }

    private static List<List<List<String>>> groupStrings(List<List<String>> checkedList, int numThreads,List<String> repEl, long time) {
        List<List<List<String>>> groupedLists = new ArrayList<>();

        boolean[] checked = new boolean[checkedList.size()];
        int currentRowNumber = 0;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        while (currentRowNumber < checkedList.size()) {
            final int currentRowNumberFinal = currentRowNumber;
            List<String> currentRow = checkedList.get(currentRowNumber);
            List<List<String>> group = new ArrayList<>();

            if (!checked[currentRowNumber]) {
                group.add(currentRow);
            }
            int refRowPos = 0;
            while (refRowPos < group.size()) {
                List<Future<?>> futures = new ArrayList<>();
                    List<Integer> result = new ArrayList<>();
                    for (int refElemPosInner = 0; refElemPosInner < group.get(refRowPos).size(); refElemPosInner++) {
                        String elementToCompare = group.get(refRowPos).get(refElemPosInner);
                        if (elementToCompare != null && !elementToCompare.equals("\"\"") && repEl.contains(elementToCompare)) {
                        final int position = refElemPosInner;
                        futures.add(executor.submit(() -> {
//                        executor.execute(()-> {
                            try {
                                for (int j = currentRowNumberFinal + 1; j < checkedList.size() - 1; j++) {
                                    if (checkedList.get(j).size() < position + 1) {
                                        break;
                                    }
                                    if (!checked[j]
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
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }));
//                        });
                    }
                }
                try {
                    for (Future<?> future : futures) {
                        future.get();
                    }
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                futures.clear();
//                    executor.shutdown();
                refRowPos++;

            }

            groupedLists.add(group);
//            if (group.size() > 1) {
//                System.out.println(groupedLists.lastIndexOf(group));
//            }
            currentRowNumber++;
        if (groupedLists.size() % 10000 == 0) {
            long timeFirst = System.currentTimeMillis() - time;
            System.out.printf("Finished %,9.3f ms\n", timeFirst / 1_000.0);
            System.out.println(groupedLists.size());
        }
//        if (currentRowNumber % 10000 == 0) {
//            System.out.println(currentRowNumber);
//        }
//            if (groupedLists.size() % 10000 == 0) {
//                break;
//            }
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