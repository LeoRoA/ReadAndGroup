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
    public static void main(String[] args) throws InterruptedException {
        System.out.println(Runtime.getRuntime().availableProcessors());
        long time = System.currentTimeMillis();
        String inputFilePath = "src/main/resources/lng-4.txt.gz";
        List<String> groups = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(new File(inputFilePath).toPath()))))) {
            String line;
            Pattern pattern = Pattern.compile("^(\"?\\w*\"?;)*(\"?\\w*\"?)+$");
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.matches()) {
                    groups.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(groups.size());

        List<List<String>> sortedGroups = new ArrayList<>();
        for (String string : groups) {
            List<String> line = Arrays.asList(string.split(";"));
            sortedGroups.add(line);
        }
        sortedGroups.sort(Comparator.comparingInt(List::size));

        int numThreads = 4; // Количество потоков для параллельной обработки
        List<List<List<String>>> groupedLists = groupStrings(sortedGroups, numThreads);
//        System.out.println(groupedLists.size());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
            int i = 1;
            for (List<List<String>> group : groupedLists) {
                writer.write("Группа " + i++ + "\n");
                for (List<String> line : group) {
                    writer.write(String.join(";", line) + "\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println(groupedLists.size());
        time = System.currentTimeMillis() - time;
        System.out.printf("Elapsed %,9.3f ms\n", time / 1_000.0);
    }

    private static List<List<List<String>>> groupStrings(List<List<String>> checkedList, int numThreads) throws InterruptedException {
        List<List<List<String>>> groupedLists = new ArrayList<>();
        boolean[] checked = new boolean[checkedList.size()];
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int currentRowNumber = checkedList.size() - 1; currentRowNumber > 0; currentRowNumber--) {
            List<String> currentRow = checkedList.get(currentRowNumber);


            final int rowNumb = currentRowNumber;

            if (!checked[rowNumb]) {
                List<List<String>> group = new ArrayList<>();
                group.add(currentRow);
                checked[rowNumb] = true;

                for (int refRowPos = 0; refRowPos < group.size(); refRowPos++) {
                    int refElemPos = 0;
                    String elementToCompare = group.get(refRowPos).get(refElemPos);

                    if (elementToCompare != null && !elementToCompare.equals("\"\"")) {
                        for (int i = 1; i < numThreads; i++) {
                            executor.execute(() -> {
                                try {
                                    for (int j = 0; j < rowNumb - 1; j++) {
                                        if (checkedList.get(j).size() > refElemPos && !checked[j] &&
                                                checkedList.get(j).get(refElemPos).equals(elementToCompare)) {
                                            group.add(checkedList.get(j));
                                            checked[j] = true;
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }

                            });
                            synchronized (group) {
                                groupedLists.add(group);
                            }
                        }

                    }


                }
//
                if (currentRowNumber % 1000 == 0) {
                    System.out.println(currentRowNumber);
                }
            }
        }
//        System.out.println(groupedLists.size());
//        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.out.println("Pool did not terminate");
                }
////            executor.awaitTermination(30, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return groupedLists;
    }

}
