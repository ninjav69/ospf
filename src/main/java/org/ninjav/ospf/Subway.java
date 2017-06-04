package org.ninjav.ospf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Subway {
	private List<Station> stations;
	private List<Connection> connections;
	private Map<Station, List<Station>> network;

	public Subway() {
		stations = new ArrayList<>();
		connections = new ArrayList<>();
		network = new HashMap<>();
	}

	public void addStation(String stationName) {
		if (!hasStation(stationName)) {
			stations.add(new Station(stationName));
		}
	}

	public void addConnection(String station1Name, String station2Name, String lineName) {
		if (hasStation(station1Name) && hasStation(station2Name)) {
			Station station1 = new Station(station1Name);
			Station station2 = new Station(station2Name);
			connections.add(new Connection(station1, station2, lineName));
			connections.add(new Connection(station2, station1, lineName));
			addToNetwork(station1, station2);
			addToNetwork(station2, station1);
		} else {
			throw new RuntimeException("Invalid connection: " + station1Name + " <-> " + station2Name);
		}
	}

	private void addToNetwork(Station station1, Station station2) {
		if (network.keySet().contains(station1)) {
			List<Station> connectingStations = network.get(station1);
			if (!connectingStations.contains(station2)) {
				connectingStations.add(station2);
			}
		} else {
			List<Station> connectingStations = new LinkedList<>();
			connectingStations.add(station2);
			network.put(station1, connectingStations);
		}
	}

	public boolean hasStation(String stationName) {
		return stations.contains(new Station(stationName));
	}

	public boolean hasConnection(String station1Name, String station2Name, String lineName) {
		Station station1 = new Station(station1Name);
		Station station2 = new Station(station2Name);
		for (Iterator<Connection> i = connections.iterator(); i.hasNext();) {
			Connection connection = i.next();
			if (connection.getLineName().equalsIgnoreCase(lineName)) {
				if ((connection.getStation1().equals(station1)) &&
						connection.getStation2().equals(station2)) {
					return true;
				}
			}
		}
		return false;
	}

	public List<Connection> getDirections(String startStationName, String endStationName) {
		if (!hasStation(startStationName) || !hasStation(endStationName)) {
			throw new RuntimeException("Stations entered to not exist on subway");
		}

		Station start = new Station(startStationName);
		Station end = new Station(endStationName);
		List<Connection> route = new LinkedList<>();
		List<Station> reachableStations = new LinkedList<>();
		Map<Station, Station> previousStations = new HashMap<>();

		List<Station> neighbors = network.get(start);
		for (Iterator<Station> i = neighbors.iterator(); i.hasNext();) {
			Station station = i.next();
			if (station.equals(end)) {
				route.add(getConnection(start, end));
				return route;
			} else {
				reachableStations.add(station);
				previousStations.put(station, start);
			}
		}

		List<Station> nextStations = new LinkedList<>();
		nextStations.addAll(neighbors);
		Station currentStation = start;

		searchLoop:
		for (int i = 1; i < stations.size(); i++) {
			List<Station> tmpNextStations = new LinkedList<>();
			for (Iterator<Station> j = nextStations.iterator(); j.hasNext();) {
				Station station = j.next();
				reachableStations.add(station);
				currentStation = station;
				List<Station> currentNeighbors = network.get(currentStation);
				for (Iterator<Station> k = currentNeighbors.iterator(); k.hasNext();) {
					Station neighbor = k.next();
					if (neighbor.equals(end)) {
						reachableStations.add(neighbor);
						previousStations.put(neighbor, currentStation);
						break searchLoop;
					} else if (!reachableStations.contains(neighbor)) {
						reachableStations.add(neighbor);
						tmpNextStations.add(neighbor);
						previousStations.put(neighbor, currentStation);
					}
				}
			}
			nextStations = tmpNextStations;
		}

		// We've found the path by now
		boolean keepLooping = true;
		Station keyStation = end;
		Station station;

		while (keepLooping) {
			station = previousStations.get(keyStation);
			route.add(0, getConnection(station, keyStation));
			if (start.equals(station)) {
				keepLooping = false;
			}
			keyStation = station;
		}

		return route;
	}

	public Connection getConnection(Station station1, Station station2) {
		for (Iterator<Connection> i = connections.iterator(); i.hasNext();) {
			Connection connection = i.next();
			Station one = connection.getStation1();
			Station two = connection.getStation2();
			if (station1.equals(one) && station2.equals(two)) {
				return connection;
			}
		}
		return null;
	}
}
