
package com.websem;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jena.rdf.model.*;

@Singleton
public class DataServlet extends WebSocketServlet implements VehicleListener {

     Property geo = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#") ;
      Property lat = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#lat") ;
      Property lon = ResourceFactory.createProperty("http://www.w3.org/2003/01/geo/wgs84_pos#long") ;

  private static final Logger _log = LoggerFactory.getLogger(DataServlet.class);

  private static final int WEB_SOCKET_TIMEOUT_MS = 120 * 1000;

  private VisualizerService visualierService;

  private Set<DataWebSocket> sockets = new ConcurrentHashSet<DataWebSocket>();

  private volatile String vehicles;

  @Inject
  public void setVisualizerService(VisualizerService visualizerService) {
    visualierService = visualizerService;
  }

  @PostConstruct
  public void start() {
    visualierService.addListener(this);
  }

  @PreDestroy
  public void stop() {
    visualierService.removeListener(this);
  }

  @Override
  public void handleVehicles(List<Vehicle> vehicles) {
    String vehiclesAsJsonString = getVehiclesAsString(vehicles);
    for (DataWebSocket socket : sockets) {
      socket.sendVehicles(vehiclesAsJsonString);
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType("application/json");
    PrintWriter writer = resp.getWriter();
    writer.write(vehicles);
  }
  
  @Override
  public WebSocket doWebSocketConnect(HttpServletRequest request,
      String protocol) {
    return new DataWebSocket();
  }

  public void addSocket(DataWebSocket dataWebSocket) {
    String vehiclesAsJsonString = getVehiclesAsString(visualierService.getAllVehicles());
    String turtle=getModelAsString(visualierService.getModel());
    dataWebSocket.sendVehicles(vehiclesAsJsonString);
    dataWebSocket.sendTurtle(turtle);
    sockets.add(dataWebSocket);
  }

  public void removeSocket(DataWebSocket dataWebSocket) {
    sockets.remove(dataWebSocket);
  }

  private String getVehiclesAsString(List<Vehicle> vehicles) {
    try {
      JSONArray array = new JSONArray();
      for (Vehicle vehicle : vehicles) {
        JSONObject obj = new JSONObject();
        obj.put("id", vehicle.getId());
        obj.put("lat", vehicle.getLat());
        obj.put("lon", vehicle.getLon());
        obj.put("lastUpdate", vehicle.getLastUpdate());
        array.put(obj);
      }
      return array.toString();
    } catch (JSONException ex) {
      throw new IllegalStateException(ex);
    }
  }
  private String getModelAsString(Model model) {
	    try {
	      JSONArray array = new JSONArray();
	      StmtIterator iter = model.listStatements();
	      while (iter.hasNext()) {
     	     Statement stmt      = iter.nextStatement();  // get next statement
        
     	    
     	     Resource  subject   = stmt.getSubject();     // get the subject
         Property  predicate = stmt.getPredicate();   // get the predicate
         RDFNode   object    = stmt.getObject();      // get the object
         JSONObject obj = new JSONObject();
	        obj.put("id", subject);
	        obj.put("lat", subject.getProperty(lat).getDouble());
	        obj.put("lon", subject.getProperty(lon).getDouble());
	        
	        array.put(obj);
        
     }
	       
	      return array.toString();
	    } catch (JSONException ex) {
	      throw new IllegalStateException(ex);
	    }
	  }

  class DataWebSocket implements WebSocket {

    private Connection _connection,conne;

    @Override
    public void onOpen(Connection connection) {
      _connection = connection;
      _connection.setMaxIdleTime(WEB_SOCKET_TIMEOUT_MS);
      addSocket(this);
    }

    @Override
    public void onClose(int closeCode, String message) {
      removeSocket(this);
    }

    public void sendVehicles(String vehiclesAsJsonString) {
      try {
        _connection.sendMessage(vehiclesAsJsonString);
       
        
      } catch (IOException ex) {
        _log.warn("error sending WebSocket message", ex);
      }
    }
    public void sendTurtle(String model) {
        try {
        	
            _connection.sendMessage(model);
           
            
          } catch (IOException ex) {
            _log.warn("error sending WebSocket message", ex);
          }
        }
  }
}
