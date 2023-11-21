
package org.testcase;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Main {
    static List<Boolean> checked = new ArrayList<>();
    static List<List<String>> inputArray = new ArrayList<>();
    static HashMap<String, List<Integer>> repeatedElements = new HashMap<>();

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
//        String inputFilePath = "src/main/resources/lng.txt";
        String inputFilePath = args[0];
        if (inputFilePath != null) {
            readFileAndGetArray(inputFilePath);
        } else {
            System.out.println("Отсутствует файл");
            System.exit(1);
        }

        List<List<List<String>>> listGroups = new ArrayList<>(groupByElements());

        System.out.println("Count group: " + listGroups.size());

        List<List<List<String>>> sortedGroups = listGroups.stream()
                .sorted(Comparator.comparingInt(List::size))
                .collect(Collectors.toList());
        Collections.reverse(sortedGroups);

        writeInFileByGroup(sortedGroups);

        long finishTime = System.currentTimeMillis() - startTime;
        System.out.printf("Finished %,9.3f ms\n", finishTime / 1_000.0);
    }

    private static void readFileAndGetArray(String inputFilePath) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(new File(inputFilePath).toPath())))) {
            String inputString;
            Pattern pattern = Pattern.compile("^(\"?\\w*\"?;)*(\"?\\w*\"?)+$");
            while ((inputString = br.readLine()) != null && !inputString.isEmpty()) {

                Matcher matcher = pattern.matcher(inputString);
                if (matcher.matches()) {
                    List<String> line = Arrays.asList(inputString.split(";"));
                    int countEmpty = 0;
                    for (String element : line) {
                        if (element != null && !element.equals("\"\"")) {
                            if (repeatedElements.containsKey(element)) {
                                int newCount = repeatedElements.get(element).get(0) + 1;
                                repeatedElements.get(element).add((inputArray.size()));
                                repeatedElements.get(element).set(0, newCount);
                            } else {
                                repeatedElements.put(element, new ArrayList<>(Arrays.asList(1, inputArray.size())));
                            }
                        } else {
                            countEmpty++;
                        }
                    }
                    if (countEmpty != line.size()) {
                        inputArray.add(line);
                        checked.add(false);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Ошибка чтения файла: " + e.getMessage());
            System.exit(1);
        }
    }

    private static List<List<List<String>>> groupByElements() {
        List<List<List<String>>> listGroups = new ArrayList<>();
        int countGroupedElements = 0;
        int linePosition = 0;
        for (List<String> line : inputArray) {
            if (!checked.get(linePosition)) {
                HashSet<List<String>> group = new HashSet<>();
                group.addAll(checkLine(line, linePosition, group));
                if (group.size() > 1) {
                    listGroups.add(new ArrayList<>(group));
                    countGroupedElements += group.size();
                }
            }
            linePosition++;
        }
        System.out.println(countGroupedElements);
        return listGroups;
    }

    private static HashSet<List<String>> checkLine(List<String> line, int linePosition, HashSet<List<String>> group) {
        int position = 0;
        if (group.contains(line)) {
            return group;
        }
        group.add(line);
        for (String element : line) {
            if (element != null && !element.equals("\"\"")) {
                if (repeatedElements.containsKey(element)) {
                    List<Integer> detectedLines = repeatedElements.get(element);
                    for (int j = 1; j < detectedLines.size(); j++) {
                        if (!inputArray.get(detectedLines.get(j)).equals(line)
                                && !checked.get(detectedLines.get(j))) {
                            List<String> checkString = inputArray.get(detectedLines.get(j));
                            if (checkString.size() < position + 1) {
                                continue;
                            }
                            if (checkString.get(position).equals(element)) {
                                checked.set(detectedLines.get(j), true);
                                group.addAll(checkLine(checkString, detectedLines.get(j), group));
                            }
                        } else if (inputArray.get(detectedLines.get(j)).equals(line) &&
                                detectedLines.get(j) != linePosition) {
                            checked.set(detectedLines.get(j), true);
                        }
                    }
                }
            }
            position++;
        }
        return group;
    }

    private static void writeInFileByGroup(List<List<List<String>>> listGroups) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
            int groupNumber = 1;
            for (List<List<String>> group : listGroups) {
                writer.write("Группа " + groupNumber++ + "\n");
                writer.write(group + "\n");
            }
            int linePos = 0;
            for (List<String> line : inputArray) {
                if (!checked.get(linePos)) {
                    writer.write("Группа " + groupNumber++ + "\n");
                    writer.write(line + "\n");
                }
                linePos++;
            }
            System.out.println(linePos);
        } catch (IOException e) {
            System.err.println("Ошибка записи файла: " + e.getMessage());
            System.exit(1);
        }
    }
}
