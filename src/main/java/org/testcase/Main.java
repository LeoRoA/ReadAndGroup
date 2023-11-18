
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
        String inputFilePath = "src/main/resources/lng-4.txt.gz";

        long time = System.currentTimeMillis();
        HashMap<String, List<Integer>> repeatedElements = new HashMap<>();
        List<List<String>> inputArray = new ArrayList<>();
        List<List<List<String>>> listGroups = new ArrayList<>();

        int maxGroupSize = 0;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(new File(inputFilePath).toPath()))))) {
            String inputString;
//            List<CompletableFuture<Void>> futures = new ArrayList<>();
            Pattern pattern = Pattern.compile("^(\"?\\w*\"?;)*(\"?\\w*\"?)+$");

            while ((inputString = br.readLine()) != null && !inputString.isEmpty()) {

                Matcher matcher = pattern.matcher(inputString);
                if (matcher.matches()) {
                    HashSet<List<String>> group = new HashSet<>();
                    List<String> line = Arrays.asList(inputString.split(";"));
                    int pos = 0;
                    for (String element : line) {
                        if (element != null && !element.equals("\"\"")) {
//                            final int posElem = pos;
//                            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
//                                synchronized (groups) {
                            if (repeatedElements.containsKey(element)) {
                                group.addAll(checkGroup(group, line, element, pos, inputArray, repeatedElements.get(element)));
                                if (group.size() <= 1) {
                                    int newValue = repeatedElements.get(element).get(0) + 1;
                                    repeatedElements.get(element).add((inputArray.size()));
                                    repeatedElements.get(element).set(0,newValue);

                                }

                            } else {
                                repeatedElements.put(element, new ArrayList<>(Arrays.asList(1, inputArray.size() )));
                            }

//                                }
//                            });
//
//                            futures.add(future);
                        }
                        pos++;
                    }
//                    try {
//                        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
//                        allOf.get();
//                    } catch (InterruptedException | ExecutionException e) {
//                        e.printStackTrace();
//                    }
                    if (group.size() > 1) {
                        listGroups.add(new ArrayList<>(group));
                        maxGroupSize=Math.max(maxGroupSize,group.size());
                    } else {
                        inputArray.add(line);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("Count group: " +listGroups.size());
        System.out.println("Max group size: " +maxGroupSize);

        long contTime = System.currentTimeMillis() - time;
        System.out.printf("Elapsed %,9.3f ms\n", contTime / 1_000.0);
        List<List<List<String>>> sortedGroups = listGroups.stream()
                .sorted(Comparator.comparingInt(List::size))
                .collect(Collectors.toList());
        Collections.reverse(sortedGroups);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
            int i = 1;
            for (List<List<String>> group : sortedGroups) {
                writer.write("Группа " + i++ + "\n");
//                        writer.write(addedLine.toString() + "\n");
                writer.write(group + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println(inputArray.size());
        time = System.currentTimeMillis() - time;
        System.out.printf("Finished %,9.3f ms\n", time / 1_000.0);
    }

    private static HashSet<List<String>> checkGroup(HashSet<List<String>> group, List<String> line, String elementToCompare,
                                                 int position, List<List<String>> inputArrayList, List<Integer> occurElement) {
        for (int j = 1; j < occurElement.size() ; j++) {
            List<String> checkString = inputArrayList.get(occurElement.get(j));
            if (checkString.size() < position + 1
                    || group.contains(checkString)) {
                continue;
            }
            if (checkString.get(position).equals(elementToCompare)) {
                group.add(checkString);
            }
        }
//        if (!group.contains(line)){
            group.add(line);
//        }
        return group;
    }
}
