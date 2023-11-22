package org.testcase;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    static List<Boolean> checked = new ArrayList<>();
    static List<List<String>> inputArray = new ArrayList<>();
    static HashMap<String, List<Integer>> repeatedElements = new HashMap<>();

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
//        String inputFilePath = "src/main/resources/lng-big.csv";
//        String inputFilePath = "src/main/resources/lng.txt";

        if (args.length!=0) {
            String inputFilePath = args[0];
        readFileAndGetArray(inputFilePath);
        } else {
            System.err.println("Не указано имя файла данных");
            System.exit(1);
        }

        List<HashSet<List<String>>> listGroups = new ArrayList<>(groupByElements());

        System.out.println("Count group: " + listGroups.size());

        List<HashSet<List<String>>> sortedGroups = listGroups.stream()
                .sorted(Comparator.comparingInt(HashSet::size))
                .collect(Collectors.toList());
        Collections.reverse(sortedGroups);

        writeInFileByGroup(sortedGroups);

        long finishTime = System.currentTimeMillis() - startTime;
        System.out.printf("Finished %,9.3f ms\n", finishTime / 1_000.0);
    }

    private static void readFileAndGetArray(String inputFilePath) {
        try (/*CSVReader csvReader = new CSVReader(*/
                BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(new File(inputFilePath).toPath())))) {
            String inputString;
            while ((inputString = br.readLine()) != null && !inputString.isEmpty()) {

                List<String> line = Arrays.asList(inputString.split(";"));
                int countEmpty = 0;
                for (String element : line) {
                    if (!element.isEmpty() && !element.equals("\"\"")) {
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
            System.out.println(inputArray.size());
        } catch (IOException e) {
            System.err.println("Ошибка чтения файла: " + e.getMessage());
            System.exit(1);
        }
    }

    private static List<HashSet<List<String>>> groupByElements() {
        List<HashSet<List<String>>> listGroups = new ArrayList<>();
        int linePosition = 0;
        for (List<String> line : inputArray) {
            if (!checked.get(linePosition)) {
                List<List<String>> group = new ArrayList<>(checkLine(line, linePosition));
                if (group.size() > 1) {
                    listGroups.add(new HashSet<>(group));
                }
            }
            linePosition++;
        }

        return listGroups;
    }

    private static List<List<String>> checkLine(List<String> line, int linePosition) {
            List<List<String>> group = new ArrayList<>();
            int countLines = 0;
            group.add(line);
            checked.set(linePosition, true);
            while (countLines < group.size()) {
                line = group.get(countLines);
                int position = 0;
                for (String element : line) {
                    if (!element.isEmpty() && !element.equals("\"\"")) {
                        if (repeatedElements.containsKey(element) && repeatedElements.get(element).get(0) != 1) {
                            List<Integer> detectedLines = repeatedElements.get(element);
                            for (int j = 1; j < detectedLines.size(); j++) {
                                int numberCheckedLine = detectedLines.get(j);
                                if (!checked.get(numberCheckedLine)
                                && !inputArray.get(numberCheckedLine).equals(line)) {
                                    List<String> checkString = inputArray.get(numberCheckedLine);
                                    if (checkString.size() - 1 < position) {
                                        continue;
                                    }
                                    if (checkString.get(position).equals(element)) {
                                        checked.set(numberCheckedLine, true);
                                        group.add(checkString);
                                    }
                                } else {
                                    checked.set(detectedLines.get(j), true);
                                }
                            }
                        }
                    }
                    position++;

                }
                countLines++;
            }
        return group;
    }

    private static void writeInFileByGroup(List<HashSet<List<String>>> listGroups) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"))) {
            int groupNumber = 1;
            for (HashSet<List<String>> group : listGroups) {
                writer.write("Группа " + groupNumber++ + "\n");
                writer.write(group.size() + "\n");
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
        } catch (IOException e) {
            System.err.println("Ошибка записи файла: " + e.getMessage());
            System.exit(1);
        }
    }
}
