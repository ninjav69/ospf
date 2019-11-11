package org.ninjav.commons.csv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class CSV {

    static public Consumer<List<String>> showCells() {
        return System.out::println;
    }

    static public void withLinesIn(final String csvFile, final Consumer<List<String>> handleLine) {
        try(Stream<String> stream = Files.lines(Paths.get(csvFile))) {
            stream.map(l -> l.replaceAll("\",\"", "|"))
                    .map(l -> l.replaceAll("^\"", ""))
                    .map(l -> l.replaceAll("\"$", ""))
                    .map(l -> Arrays.asList(l.split("\\|", -1)))
                    .forEach(cells -> handleLine.accept(cells));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
