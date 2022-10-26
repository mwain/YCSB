package site.ycsb.db;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * YCSB binding for <a href="https://sanity.io/">Sanity</a>.
 */
public class SanityClient extends site.ycsb.DB {
  public enum MutationType {
    CREATE("create"),
    CREATE_OR_REPLACE("createOrReplace"),
    CREATE_IF_NOT("createIfNotExists"),
    PATCH("patch"),
    DELETE("delete");

    private final String name;

    MutationType(final String name) {
      this.name = name;
    }
  }

  private static final String PROJECT = "sanity.project";
  private static final String DATASET = "sanity.dataset";
  private static final String API_PROTOCOL = "sanity.api.protocol";
  private static final String API_HOST = "sanity.api.host";
  private static final String API_VERSION = "sanity.api.version";
  private static final String API_AUTH_TOKEN = "sanity.api.auth_token";
  private static final String READ_TYPE = "sanity.query.read";
  private static final String MUTATION_VISIBILITY = "sanity.mutation.visibility";


  private static final String DEFAULT_API_PROTOCOL = "https://";
  private static final String DEFAULT_API_HOST = "api.sanity.io";
  private static final String DEFAULT_API_VERSION = "vX";
  private static final String DEFAULT_READ_TYPE = "guery";
  private static final String DEFAULT_MUTATION_VISIBILITY = "sync";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String dataset;
  private String apiVersion;
  private String apiAuthToken;
  private String readType;
  private String mutationVisibility;

  private String url;

  private HttpClient httpClient;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    var project = props.getProperty(PROJECT, "");
    dataset = props.getProperty(DATASET, "");
    var apiProtocol = props.getProperty(API_PROTOCOL, DEFAULT_API_PROTOCOL);
    var apiHost = props.getProperty(API_HOST, DEFAULT_API_HOST);
    apiVersion = props.getProperty(API_VERSION, DEFAULT_API_VERSION);
    apiAuthToken = props.getProperty(API_AUTH_TOKEN, "");
    readType = props.getProperty(READ_TYPE, DEFAULT_READ_TYPE);
    mutationVisibility = props.getProperty(MUTATION_VISIBILITY, DEFAULT_MUTATION_VISIBILITY);

    url = apiProtocol;
    if (!Objects.equals(project, "")) {
      url += project + ".";
    }
    url += apiHost;

    httpClient = HttpClient.newHttpClient();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    var query = new StringBuilder("*[_type == '%s' && _id == '%s']".formatted(table, key));

    if (fields != null && fields.size() > 0) {
      query.append("{");
      for (String field : fields) {
        query.append(field).append(",");
      }
      query.append("}");
    }

    var request = HttpRequest.newBuilder()
        .GET()
        .uri(URI.create(url + "/" + apiVersion + "/" + "query/" + dataset + "?query=" + encodeValue(query.toString())))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", "Bearer " + apiAuthToken)
        .build();

    HttpResponse<String> response = null;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (response.statusCode() >= 300) {
      return getStatus(response.statusCode());
    }

    JsonNode rootNode = null;
    try {
      rootNode = MAPPER.readTree(response.body());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (rootNode == null) {
      return Status.UNEXPECTED_STATE;
    }

    JsonNode resultNode = rootNode.get("result");
    if (resultNode == null) {
      return Status.NOT_FOUND;
    }

    if (fields == null) {
      return getStatus(response.statusCode());
    }

    Iterator<JsonNode> resultIt = resultNode.getElements();
    while (resultIt.hasNext()) {
      JsonNode valueNode = resultIt.next();
      for (String field : fields) {
        JsonNode fieldNode = valueNode.get(field);
        if (fieldNode == null) {
          continue;
        }

        result.put(field, new StringByteIterator(fieldNode.asText()));
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    var query = new StringBuilder("*[_type == '%s' && _id >= '%s'] | order(_id ) [0..%d]".formatted(
        table, startkey, recordcount));

    if (fields != null && fields.size() > 0) {
      query.append("{");
      for (String field : fields) {
        query.append(field).append(",");
      }
      query.append("}");
    }

    var request = HttpRequest.newBuilder()
        .GET()
        .uri(URI.create(url + "/" + apiVersion + "/" + "query/" + dataset + "?query=" + encodeValue(query.toString())))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", "Bearer " + apiAuthToken)
        .build();

    HttpResponse<String> response = null;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (response.statusCode() >= 300) {
      return getStatus(response.statusCode());
    }

    JsonNode rootNode = null;
    try {
      rootNode = MAPPER.readTree(response.body());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (rootNode == null) {
      return Status.UNEXPECTED_STATE;
    }

    JsonNode resultNode = rootNode.get("result");
    if (resultNode == null) {
      return Status.NOT_FOUND;
    }

    if (fields == null) {
      return getStatus(response.statusCode());
    }

    Iterator<JsonNode> resultIt = resultNode.getElements();
    while (resultIt.hasNext()) {
      final HashMap<String, ByteIterator> map = new HashMap<>();
      JsonNode valueNode = resultIt.next();

      for (String field : fields) {
        JsonNode fieldNode = valueNode.get(field);
        if (fieldNode == null) {
          continue;
        }

        map.put(field, new StringByteIterator(fieldNode.asText()));
      }

      result.add(map);
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    var mutation = generateMutation(MutationType.PATCH, table, key, values);
    var request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(mutation))
        .uri(URI.create(url + "/" + apiVersion + "/" + "mutate/" + dataset))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", "Bearer " + apiAuthToken)
        .build();

    HttpResponse<String> response = null;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return getStatus(response.statusCode());
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    var mutation = generateMutation(MutationType.CREATE, table, key, values);
    var request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(mutation))
        .uri(URI.create(url + "/" + apiVersion + "/" + "mutate/" + dataset + "?visibility=" + mutationVisibility))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", "Bearer " + apiAuthToken)
        .build();

    HttpResponse<String> response = null;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return getStatus(response.statusCode());
  }

  @Override
  public Status delete(String table, String key) {
    var mutation = generateMutation(MutationType.CREATE, table, key, null);
    var request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(mutation))
        .uri(URI.create(url + "/" + apiVersion + "/" + "mutate/" + dataset))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", "Bearer " + apiAuthToken)
        .build();

    HttpClient client = HttpClient.newHttpClient();

    HttpResponse<String> response = null;
    try {
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return getStatus(response.statusCode());
  }

  private String generateMutation(MutationType mutationType, String type, String id, Map<String, ByteIterator> values) {
    ObjectNode mutationNode = switch (mutationType) {
      case CREATE, CREATE_OR_REPLACE, CREATE_IF_NOT: {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("_id", id);
        node.put("_type", type);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          node.put(entry.getKey(), entry.getValue().toArray());
        }
        yield node;
      }
      case PATCH: {
        ObjectNode setNode = MAPPER.createObjectNode();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          setNode.put(entry.getKey(), entry.getValue().toArray());
        }

        ObjectNode node = MAPPER.createObjectNode();
        node.put("id", id);
        node.put("set", setNode);
        yield node;
      }
      case DELETE:
        ObjectNode node = MAPPER.createObjectNode();
        node.put("_id", id);
        yield node;
    };

    ObjectNode mutationItemNode = MAPPER.createObjectNode();
    mutationItemNode.put(mutationType.name, mutationNode);

    ArrayNode mutationsArrayNode = MAPPER.createArrayNode();
    mutationsArrayNode.add(mutationItemNode);

    ObjectNode rootNode = MAPPER.createObjectNode();
    rootNode.put("mutations", mutationsArrayNode);

    JsonFactory factory = new JsonFactory();
    Writer writer = new StringWriter();
    try {
      JsonGenerator jsonGenerator = factory.createJsonGenerator(writer);
      MAPPER.writeTree(jsonGenerator, rootNode);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return writer.toString();
  }

  private Status getStatus(int responseCode) {
    if (responseCode >= 200 && responseCode < 300) return Status.OK;
    if (responseCode == 400) return Status.BAD_REQUEST;
    if (responseCode == 403) return Status.FORBIDDEN;
    if (responseCode == 404) return Status.NOT_FOUND;
    if (responseCode == 501) return Status.NOT_IMPLEMENTED;
    if (responseCode == 503) return Status.SERVICE_UNAVAILABLE;

    return Status.ERROR;
  }

  private String encodeValue(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}