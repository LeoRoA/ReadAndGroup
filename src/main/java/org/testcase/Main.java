package org.testcase;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class Main {
    public static void main(String[] args) {
//        if (args.length != 1) {
//            System.out.println("Usage: java -jar <название проекта>.jar <тестовый-файл.txt>");
//            System.exit(1);
//        }
        long time = System.currentTimeMillis();
        String inputFilePath = "src/main/resources/lng-4.txt.gz";
//        Map<Integer, Set<List<String>>> groups = new HashMap<>();
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
////        List<Map.Entry<Integer, Set<List<String>>>> sortedGroups = new ArrayList<>(groups.entrySet());
////        sortedGroups.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey()));
////        System.out.println("sg: " + sortedGroups.size());
//
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
//            int groupCount = 0;
////            int sizeCount = sortedGroups.get(0).getKey() + 1;
//            List<List<String>> addedGroup = new ArrayList<>();
//
//            // исправить на итератор!!!!!!!!!!
//            Iterator<List<String>> iterator = sortedGroups.iterator();
//            int fixSize = sortedGroups.size();
//            for (int i=fixSize-1; i>0; i--){
//                groupCount++;
//                List<String> line = sortedGroups.get(i);
//                addedGroup.add(line);
//                writer.write("Группа " + groupCount + "\n");
//                writer.write(line + "\n");
//
//                for (int j = 0; j < line.size() - 1; j++) {
//                    final int position = j;
//                    addedGroup.addAll(sortedGroups.stream()
//                                    .
//                            .filter(e -> (e.size() - 1 <= position && e.get(position).equals(line.get(position))))
//                            .collect(Collectors.toList()));
//
//                }
//                writer.write("Группа " + groupCount + "\n");
//                    for (List<String> addedLine : addedGroup) {
//                        writer.write(addedLine.toString() + "\n");
//                    }
////                writer.write(group + "\n");
//
//
//            }
//            System.out.println("Количество групп с более чем одним элементом: " + groupCount);
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }


//        for (int i = 0; i < groups.size(); i++) {
//            Set<List<String>> groupA = groups.get(i);
//
//            for (int j = i + 1; j < groups.size(); j++) {
//                Set<List<String>> groupB = groups.get(j);
//
//                if (haveCommonValuesAtSamePositions(groupA, groupB)) {
//                    groupA.addAll(groupB);
//                    groups.remove(j);
//                    j--;
//                }
//            }
//        }
        List<List<List<String>>> result = groupStrings(data, 1);



        time = System.currentTimeMillis() - time;
        System.out.printf("Elapsed %,9.3f ms\n", time / 1_000.0);
    }
    private static List<List<String>> groupStrings(List<String> addedLine, List<List<String>>checkedList){

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
            List<List<String>> addedGroup = new ArrayList<>();

            while (checkedList.size()!=0){
                List<String> addedLine = checkedList.get(0);
                checkedList.remove(0);
                addedGroup.add(addedLine);
//                int lineSize = addedGroup.get(i).size();
                for (int i = 0; i<addedLine.size()-1;i++) {
                    String exitingString = addedLine.get(i);
                    for (List<String> checkedLine: checkedList){
                        if (checkedLine.size()>=i && checkedLine.get(0).equals(exitingString){
                            addedGroup.add(checkedLine);
                        }
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }




    }
}
