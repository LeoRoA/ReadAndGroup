
package org.testcase;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


public class Main {
    static List<Boolean> checked = new ArrayList<>();
    static List<List<String>> inputArray = new ArrayList<>();
    static HashMap<String, List<Integer>> repeatedElements = new HashMap<>();
    static int maxGroupSize = 0;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String urlFilePath = "https://github.com/PeacockTeam/new-job/releases/download/v1.0/lng-4.txt.gz";

        String downloadedFilePath = null;
        if (args.length==0) {
            try {
                downloadedFilePath = downloadFile(urlFilePath);
                System.out.println("Файл успешно загружен.");
            } catch (IOException e) {
                System.err.println("Ошибка загрузки файла: " + e.getMessage());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        else{
            downloadedFilePath = args[0];
        }

        if (downloadedFilePath != null) {

            try {
                readFileAndGetArray(downloadedFilePath);
            }catch (IOException e){
                e.printStackTrace();
            }

        } else {
            System.out.println("Отсутствует файл");
            System.exit(1);
        }

        List<List<List<String>>> listGroups = new ArrayList<>(groupByElements());

        System.out.println("Count group: " + listGroups.size());
//
//        List<List<List<String>>> sortedGroups = listGroups.stream()
//                .sorted(Comparator.comparingInt(List::size))
//                .collect(Collectors.toList());
//        Collections.reverse(sortedGroups);

        writeInFileByGroup(listGroups);

        long finishTime = System.currentTimeMillis() - startTime;
        System.out.printf("Finished %,9.3f ms\n", finishTime / 1_000.0);
    }
    private static String downloadFile(String fileUrl) throws IOException, URISyntaxException {
        URL url = new URL(fileUrl);

        String fileName = Paths.get(url.getPath()).getFileName().toString();
        String pathJar = Main.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath();
        Path downloadsPath = Paths.get(new File(pathJar).getParent());
        if (!downloadsPath.toFile().exists()){
            downloadsPath.toFile().mkdir();
        }

        Path filePath = downloadsPath.resolve(fileName);
        try (InputStream in = url.openStream();
             FileOutputStream out = new FileOutputStream(filePath.toString())) {

            byte[] buffer = new byte[1024];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        return filePath.toString();
    }
    private static void readFileAndGetArray(String downloadedFilePath) throws IOException {
        InputStream inputStream;
        if (downloadedFilePath.contains(".gz")){
            inputStream = new GZIPInputStream(Files.newInputStream(new File(downloadedFilePath).toPath()));
        } else{
            inputStream = Files.newInputStream(new File(downloadedFilePath).toPath());
    }
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(inputStream) )){
            String inputString;
            Pattern pattern = Pattern.compile("^(\"?\\w*\"?;)*(\"?\\w*\"?)+$");
            while ((inputString = br.readLine()) != null && !inputString.isEmpty()) {

                Matcher matcher = pattern.matcher(inputString);
                if (matcher.matches()) {
                    boolean alreadyContains = true;
                    List<String> line = Arrays.asList(inputString.split(";"));
                    for (String element : line) {
                        if (element != null && !element.equals("\"\"")) {
                            if (repeatedElements.containsKey(element)) {
                                for (int alreadyAdded = 1; (alreadyAdded < repeatedElements.get(element).size()
                                        && repeatedElements.get(element).get(alreadyAdded) != inputArray.size()); alreadyAdded++) {
                                    if (!line.equals(inputArray.get(repeatedElements.get(element).get(alreadyAdded)))) {
                                        int newCount = repeatedElements.get(element).get(0) + 1;
                                        repeatedElements.get(element).add((inputArray.size()));
                                        repeatedElements.get(element).set(0, newCount);
                                        alreadyContains = false;
                                    } else {
                                        alreadyContains = true;
                                        break;
                                    }
                                }
                            } else {
                                repeatedElements.put(element, new ArrayList<>(Arrays.asList(1, inputArray.size())));
                                alreadyContains = false;
                            }
                        }
                    }
                    if (!alreadyContains) {
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
        int linePosition = 0;
        for (List<String> line : inputArray) {
            if (!checked.get(linePosition)) {
                HashSet<List<String>> group = new HashSet<>();
                group.addAll(checkLine(line, group));
                if (group.size() > 1) {
                    listGroups.add(new ArrayList<>(group));
                }
            }

            linePosition++;
        }
        return listGroups;
    }

    private static HashSet<List<String>> checkLine(List<String> line, HashSet<List<String>> group) {
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
                                group.addAll(checkLine(checkString, group));
                            }
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
        } catch (IOException e) {
            System.err.println("Ошибка записи файла: " + e.getMessage());
            System.exit(1);
        }
    }




}
