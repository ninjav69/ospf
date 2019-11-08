package org.ninjav.ospf;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class SubwayLoaderTest {
    @Test
    public void canLoadSubwayFile() throws IOException {
        File subwayFile = getFileFromResources("ObjectvilleSubway.txt");
        SubwayLoader loader = new SubwayLoader();
        Subway subway = loader.loadFromFile(subwayFile);
        assertThat(subway, is(notNullValue()));

        assertThat(subway.hasStation("Ajax Rapids"), is(true));
        assertThat(subway.hasStation("XHTML Expressway"), is(true));

        assertThat(subway.hasConnection("UML Walk", "Objectville PizzaStore", "Booch Line"), is(true));
        assertThat(subway.hasConnection("UML Walk", "HeadFirstLabs", "Booch Line"), is(false));
    }

    @Test
    public void canPrintRoute() throws IOException {
        File subwayFile = getFileFromResources("ObjectvilleSubway.txt");
        SubwayLoader loader = new SubwayLoader();
        Subway subway = loader.loadFromFile(subwayFile);
        List<Connection> route = subway.getDirections("Algebra Avenue", "PMP Place");
        SubwayPrinter printer = new SubwayPrinter(System.out);
        printer.printDirections(route);
    }


    private File getFileFromResources(String filename) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(filename).getFile());
    }
}
