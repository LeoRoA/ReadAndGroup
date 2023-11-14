
package org.testcase;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


public class Main {
    public static void main(String[] args) {
        long time = System.currentTimeMillis();
//        String inputFilePath = "src/main/resources/lng-4.txt.gz";
        String inputFilePath = "src/main/resources/lng.txt";
        HashSet<String> groups = new HashSet<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(inputFilePath).toPath())))){
            //                        new GZIPInputStream(Files.newInputStream(new File(inputFilePath).toPath()))))) {
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
        Collections.reverse(sortedGroups);
        int numThreads = sortedGroups.get(0).size(); // Количество потоков для параллельной обработки
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<List<List<String>>> groupedLists = new ArrayList<>();
        try {
            groupedLists = groupStrings(sortedGroups, executor);
            if (executor.awaitTermination(25, TimeUnit.SECONDS)) {

                executor.shutdown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


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
        System.out.printf("Elapsed %,9.3f ms\n", time / 1_000.0);
    }

    private static List<List<List<String>>> groupStrings(List<List<String>> checkedList, ExecutorService executor) {
        List<List<List<String>>> groupedLists = new ArrayList<>();

        boolean[] checked = new boolean[checkedList.size()];
        AtomicInteger currentRowNumber = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();
        while (currentRowNumber.get() < checkedList.size()) {
            if (currentRowNumber.get() == -1) {
                System.out.println("bla");
            }
            final int curRowNumFinal = currentRowNumber.get();
            List<String> currentRow = checkedList.get(currentRowNumber.get());
            List<List<String>> group = new ArrayList<>();

            if (!checked[curRowNumFinal]) {
                checked[curRowNumFinal] = true;
                group.add(currentRow);
            } else {
                currentRowNumber.getAndIncrement();
                continue;
            }

            int refRowPos = 0;
            while (refRowPos < group.size()) {
                List<Integer> result = new ArrayList<>();

                for (int refElemPosInner = 0; refElemPosInner < group.get(refRowPos).size(); refElemPosInner++) {
                    String elementToCompare = group.get(refRowPos).get(refElemPosInner);
                    if (elementToCompare != null && !elementToCompare.equals("\"\"")) {
                        final int position = refElemPosInner;
                        futures.add(executor.submit(() -> {

                            try {
                                synchronized (checked) {
                                    for (int j = currentRowNumber.get() + 1; j < checkedList.size() - 1; j++) {
                                        try{
                                        if (checkedList.get(j).size() < position+1) {

                                            break;
                                        }
                                        if (!checked[j]
                                                && checkedList.get(j).get(position).equals(elementToCompare)) {
                                            result.add(j);
                                        }
                                        if (!result.isEmpty()) {
                                            for (int pos : result) {
                                                checked[pos] = true;
                                                group.add(checkedList.get(pos));
                                            }
                                        }
                                        }catch (ArrayIndexOutOfBoundsException e) {
                                            System.out.println(j + " :" + position);
                                            e.printStackTrace();
                                        }

                                    }
                                }



                        }catch (Exception e) {
                                e.printStackTrace();
                            }
                        }));
                    }

                }

                refRowPos++;
                futures.clear();
            }
            for (Future<?> future : futures) {
                try {
                    future.get(); // Ждем завершения всех потоков
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            groupedLists.add(group);
            currentRowNumber.getAndIncrement();
            if (group.size() > 1) {
                System.out.println(groupedLists.lastIndexOf(group));
            }


        if (groupedLists.size() % 100000 == 0) {
            System.out.println(groupedLists.size());
        }
//        if (currentRowNumber % 10000 == 0) {
//            System.out.println(currentRowNumber);
//        }
        }
//        Collections.reverse(groupedLists);

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