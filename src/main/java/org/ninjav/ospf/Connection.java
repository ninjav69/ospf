package org.ninjav.ospf;

public class Connection {
	private Station station1;
	private Station station2;
	private String lineName;

	public Connection(Station station1, Station station2, String lineName) {
		this.station1 = station1;
		this.station2 = station2;
		this.lineName = lineName;
	}

	public Station getStation1() {
		return station1;
	}

	public void setStation1(Station station1) {
		this.station1 = station1;
	}

	public Station getStation2() {
		return station2;
	}

	public void setStation2(Station station2) {
		this.station2 = station2;
	}

	public String getLineName() {
		return lineName;
	}

	public void setLineName(String lineName) {
		this.lineName = lineName;
	}
}
