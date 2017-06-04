package org.ninjav.ospf;

import java.util.Objects;

public class Station {

	private String name;

	public Station(String stationName) {
		this.name = stationName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Station)) return false;
		Station station = (Station)o;
		return Objects.equals(name, station.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}
}
