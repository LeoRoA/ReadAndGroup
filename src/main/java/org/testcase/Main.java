
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
//        HashSet<String> groups = new HashSet<>();
        HashMap<String, Integer> repeatedElements = new HashMap<>();
        HashSet<List<String>> inputArray = new HashSet<>();
        HashSet<Integer> repElemPositions = new HashSet<>();
        List<List<String>> groups = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(new File(inputFilePath).toPath()))))) {
            String inputString;
            Pattern pattern = Pattern.compile("^(\"?\\w*\"?;)*(\"?\\w*\"?)+$");
            int i = 0;
            while ((inputString = br.readLine()) != null && !inputString.isEmpty()) {
                Matcher matcher = pattern.matcher(inputString);
                if (matcher.matches()) {
//                    List<String> elements = Arrays.asList(line.split(";"));
                    List<String> line = Arrays.asList(inputString.split(";"));
                    for (String element : line) {
                        if (repeatedElements.containsKey(element)) {
                            int newValue = repeatedElements.get(element) + 1;
                            repeatedElements.put(element, newValue);
                            repElemPositions.add(inputArray.size());
                            groups.addAll(checkGroup(line,element,inputArray));
                            if (groups)
                        } else {
                            repeatedElements.put(element, 1);
                        }
                    }
                    inputArray.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(inputArray.size());

        // Sort the groups by size in descending order
        List<List<String>> sortedGroups = new ArrayList<>();
        for (String string : groups) {
//            List<String> line = Arrays.asList(string.split(";"));
//            for (String element : line) {
//                if (repeatedElements.containsKey(element)) {
//                    int newValue = repeatedElements.get(element) + 1;
//                    repeatedElements.put(element, newValue);
//                } else {
//                    repeatedElements.put(element, 1);
//                }
//            }
            sortedGroups.add(line);
        }

        List<String> repEl = repeatedElements.entrySet()
                .stream()
                .filter(x -> x.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        long contTime = System.currentTimeMillis() - time;
        System.out.printf("Elapsed %,9.3f ms\n", contTime / 1_000.0);
        List<List<String>> sortedInput = inputArray.stream()
                .sorted(Comparator.comparingInt(List::size))
                .collect(Collectors.toList());
        Collections.reverse(sortedInput);
//
//        int numThreads = 11; // Количество потоков для параллельной обработки
//        List<List<List<String>>> groupedLists = new ArrayList<>(groupStrings(sortedInput, numThreads, repEl, time, repElemPositions));
//        groupedLists.sort(Comparator.comparingInt(List::size));
//           Collections.reverse(groupedLists);
//
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
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
//
//    private static List<List<List<String>>> groupStrings(List<List<String>> checkedList, int numThreads,
//                                                         List<String> repEl, long time, HashSet repElemPosition) {
//        List<List<List<String>>> groupedLists = new ArrayList<>();
//        ArrayList<Integer> repElemPos = new ArrayList<>(repElemPosition);
//        boolean[] checked = new boolean[checkedList.size()];
//        int currentRowNumber = 0;
////        ExecutorService executorElement = Executors.newFixedThreadPool(1);
////        ExecutorService executorRows = Executors.newFixedThreadPool(4);
//        while (currentRowNumber < checkedList.size()) {
//
////            final int currentRowNumberFinal = currentRowNumber;
//            List<String> currentRow = checkedList.get(currentRowNumber);
//            List<List<String>> group = new ArrayList<>();
//
//            if (checked[currentRowNumber]) {
//                currentRowNumber++;
//                continue;
//            }
//            group.add(currentRow);
//            group.addAll(checkGroup(currentRow,currentRowNumber,repEl,repElemPos,checkedList,checked));
//
//
//            groupedLists.add(group);
////            if (group.size() > 1) {
////                System.out.println(groupedLists.lastIndexOf(group));
////            }
//            currentRowNumber++;
//            if (groupedLists.size() % 100000 == 0) {
////                long timeFinish = System.currentTimeMillis() - time;
////                System.out.printf("Finished %,9.3f ms\n", timeFinish / 1_000.0);
//                System.out.println(groupedLists.size());
//            }
////        if (currentRowNumber % 1000 == 0) {
//////            System.out.println(currentRowNumber);
////            long timeFinish = System.currentTimeMillis() - time;
////            System.out.printf("Finished 1k %,9.3f ms\n", timeFinish / 1_000.0);
////        }
////            if (groupedLists.size() % 100000 == 0) {
////                System.out.println("curnum = " + currentRowNumber);
////                break;
////            }
//        }
//
//        return groupedLists;
//    }
//
    private static List<List<String>> checkGroup (List<String> currentRow, int currentRowNumber,
                                                  List<String> repEl, List<Integer> repElemPos,
                                                  List<List<String>> checkedList, boolean [] checked){
            List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<List<String>> group = new ArrayList<>();
        List<Integer> result = new ArrayList<>();

            for (int refElemPosInner = 0; refElemPosInner < currentRow.size(); refElemPosInner++) {
                String elementToCompare = currentRow.get(refElemPosInner);
                if (elementToCompare != null && !elementToCompare.equals("\"\"") && repEl.contains(elementToCompare)) {
                    final int position = refElemPosInner;
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//                                    synchronized (group) {

//                                        for (int thread = 1; thread <= 4; thread++) {
//                                            int start = (thread - 1) * repElemPosition.size() / numThreads;
//                                            int finish = thread * repElemPosition.size() / numThreads - 1;
//                                            CompletableFuture<Void> futureInner = CompletableFuture.runAsync(() -> {

//                        executor.execute(()-> {
                        try {

                            synchronized (group) {
                                for (int j = 0; j < repElemPos.size() - 1; j++) {
                                    if (checkedList.get(repElemPos.get(j)).size() < position + 1) {
                                        break;
                                    }
                                    if (repElemPos.get(j) > currentRowNumber
                                            && !checked[repElemPos.get(j)]
                                            && checkedList.get(repElemPos.get(j)).get(position).equals(elementToCompare)) {
                                        result.add(repElemPos.get(j));
                                        checked[repElemPos.get(j)] = true;
//                                            if (result.size() == 1) {
//                                                System.out.println("tro");
//                                            }
                                    }
                                }
                                for (int pos : result) {
                                    group.add(checkedList.get(pos));
                                    group.addAll(checkGroup(checkedList.get(pos), pos, repEl, repElemPos, checkedList, checked));
                                }
                            }
//                                    long finishThread = System.currentTimeMillis()-startThread;
//                                    System.out.printf("Finish %,9.3f ms\n", finishThread / 1_000.0);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
//                        }));
//                                            }, executorRows);
//                                            futures.add(futureInner);
//                                        }
//                                    }
                    });
                    futures.add(future);
                }
            }
//            try {
//
//                CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
//                allOf.get();
//            } catch (InterruptedException | ExecutionException e) {
//                e.printStackTrace();
//            }

//                    executor.shutdown();

//                }
//                long finishThread = System.currentTimeMillis()-startThread;
//                System.out.printf("Finish %,9.3f ms\n", finishThread / 1_000.0);
        return group;
//    }
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