/**
 * Copyright (C) 2012 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.websem;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.inject.Singleton;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.eclipse.jetty.websocket.WebSocket.Connection;
import org.eclipse.jetty.websocket.WebSocket.OnBinaryMessage;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.VehicleDescriptor;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;

@Singleton
public class VisualizerService {

  private static final Logger _log = LoggerFactory.getLogger(VisualizerService.class);

  private URI vehiclePositionsUri;

  private ScheduledExecutorService executor;

  private WebSocketClientFactory webSocketFactory;

  private WebSocketClient webSocketClient;

  private IncrementalWebSocket incrementalWebSocket;

  private Future<Connection> webSocketConnection;

  private Map<String, String> vehicleIdsByEntityIds = new HashMap<String, String>();

  private Map<String, Vehicle> vehiclesById = new ConcurrentHashMap<String, Vehicle>();

  private Model model = ModelFactory.createDefaultModel();
  private Property geo = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#") ;
  private Property lat = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat") ;
  private Property lon = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long") ;

  
  private List<VehicleListener> listeners = new CopyOnWriteArrayList<VehicleListener>();

  private final RefreshTask refreshTask = new RefreshTask();

  private int refreshInterval = 20;

  private boolean dynamicRefreshInterval = true;

  private long mostRecentRefresh = -1;

  public void setVehiclePositionsUri(URI uri) {
    vehiclePositionsUri = uri;
  }

  @PostConstruct
  public void start() throws Exception {
    String scheme = vehiclePositionsUri.getScheme();
    if (scheme.equals("ws") || scheme.equals("wss")) {
      webSocketFactory = new WebSocketClientFactory();
      webSocketFactory.start();
      webSocketClient = webSocketFactory.newWebSocketClient();
      webSocketClient.setMaxBinaryMessageSize(16384000); 
      incrementalWebSocket = new IncrementalWebSocket();
      webSocketConnection = webSocketClient.open(vehiclePositionsUri,
          incrementalWebSocket);
    } else {
      executor = Executors.newSingleThreadScheduledExecutor();
      executor.schedule(refreshTask, 0, TimeUnit.SECONDS);
    }
  }

  @PreDestroy
  public void stop() throws Exception {
    if (webSocketConnection != null) {
      webSocketConnection.cancel(false);
    }
    if (webSocketClient != null) {
      webSocketClient = null;
    }
    if (webSocketFactory != null) {
      webSocketFactory.stop();
      webSocketFactory = null;
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  public List<Vehicle> getAllVehicles() {
    return new ArrayList<Vehicle>(vehiclesById.values());
  }
  public Model getModel() {
	    return model;
	  }
  public void addListener(VehicleListener listener) {
    listeners.add(listener);
  }

  public void removeListener(VehicleListener listener) {
    listeners.remove(listener);
  }

  private void refresh() throws IOException {

    _log.info("refreshing vehicle positions");

    URL url = vehiclePositionsUri.toURL();
    FeedMessage feed = FeedMessage.parseFrom(url.openStream());

    boolean hadUpdate = processDataset(feed);

    if (hadUpdate) {
      if (dynamicRefreshInterval) {
        updateRefreshInterval();
      }
    }

    executor.schedule(refreshTask, refreshInterval, TimeUnit.SECONDS);
  }

  private boolean processDataset(FeedMessage feed) {

    List<Vehicle> vehicles = new ArrayList<Vehicle>();
    boolean update = false;

    for (FeedEntity entity : feed.getEntityList()) {
      if (entity.hasIsDeleted() && entity.getIsDeleted()) {
        String vehicleId = vehicleIdsByEntityIds.get(entity.getId());
        if (vehicleId == null) {
          _log.warn("unknown entity id in deletion request: " + entity.getId());
          continue;
        }
        vehiclesById.remove(vehicleId);
        continue;
      }
      if (!entity.hasVehicle()) {
        continue;
      }
      VehiclePosition vehicle = entity.getVehicle();
      String vehicleId = getVehicleId(vehicle);
      if (vehicleId == null) {
        continue;
      }
      vehicleIdsByEntityIds.put(entity.getId(), vehicleId);
      if (!vehicle.hasPosition()) {
        continue;
      }
      Position position = vehicle.getPosition();
      Vehicle v = new Vehicle();
      v.setId(vehicleId);
      v.setLat(position.getLatitude());
      v.setLon(position.getLongitude());
      v.setLastUpdate(System.currentTimeMillis());

      Vehicle existing = vehiclesById.get(vehicleId);
      String   vehicleURI ="http://somewhere/vehicle"+vehicleId;
      
      if (existing == null || existing.getLat() != v.getLat()
          || existing.getLon() != v.getLon()) {
        vehiclesById.put(vehicleId, v);
        update = true;
        if(existing==null){
        	org.apache.jena.rdf.model.Resource vehiclerdf = model.createResource(vehicleURI)
                    .addProperty(lat,Double.toString(v.getLat()))
                    .addProperty(lon, Double.toString( v.getLon()));
        }
        org.apache.jena.rdf.model.Resource r=model.getResource(vehicleURI);
        if(r.getProperty(lat).getDouble() != v.getLat()){
            r.removeAll(lon);
            r.addProperty(lon, Double.toString(v.getLon()));
            }
            if(r.getProperty(lon).getDouble() != v.getLon()){
            	r.removeAll(lat);
            	r.addProperty(lat, Double.toString(v.getLat()));
            }
      } else {
        v.setLastUpdate(existing.getLastUpdate());
      }

      vehicles.add(v);
    }

    if (update) {
      _log.info("vehicles updated: " + vehicles.size());
    }

    for (VehicleListener listener : listeners) {
      listener.handleVehicles(vehicles);
    }

    return update;
  }

  /**
   * @param vehicle
   * @return
   */
  private String getVehicleId(VehiclePosition vehicle) {
    if (!vehicle.hasVehicle()) {
      return null;
    }
    VehicleDescriptor desc = vehicle.getVehicle();
    if (!desc.hasId()) {
      return null;
    }
    return desc.getId();
  }

  private void updateRefreshInterval() {
    long t = System.currentTimeMillis();
    if (mostRecentRefresh != -1) {
      int refreshInterval = (int) ((t - mostRecentRefresh) / (2 * 1000));
      refreshInterval = Math.max(10, refreshInterval);
      _log.info("refresh interval: " + refreshInterval);
    }
    mostRecentRefresh = t;
  }

  private class RefreshTask implements Runnable {
    @Override
    public void run() {
      try {
        refresh();
      } catch (Exception ex) {
        _log.error("error refreshing GTFS-realtime data", ex);
      }
    }
  }

  private class IncrementalWebSocket implements OnBinaryMessage {

    @Override
    public void onOpen(Connection connection) {

    }

    @Override
    public void onMessage(byte[] buf, int offset, int length) {
      if (offset != 0 || buf.length != length) {
        byte trimmed[] = new byte[length];
        System.arraycopy(buf, offset, trimmed, 0, length);
        buf = trimmed;
      }
      FeedMessage message = parseMessage(buf);
      FeedHeader header = message.getHeader();
      switch (header.getIncrementality()) {
        case FULL_DATASET:
          processDataset(message);
          break;
        case DIFFERENTIAL:
          processDataset(message);
          break;
        default:
          _log.warn("unknown incrementality: " + header.getIncrementality());
      }
    }

    @Override
    public void onClose(int closeCode, String message) {

    }

    private FeedMessage parseMessage(byte[] buf) {
      try {
        return FeedMessage.parseFrom(buf);
      } catch (InvalidProtocolBufferException ex) {
        throw new IllegalStateException(ex);
      }
    }
  }
}
