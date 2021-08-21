package io.pravega.writer;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class PravegaWriter {

  private static EventStreamWriter<String> createWriter(
      String controllerUri, String scope, String inputStream, String outputStream) {
    final URI controllerURI = URI.create(controllerUri);
    try (StreamManager streamManager = StreamManager.create(controllerURI)) {
      if (!streamManager.checkScopeExists(scope)) {
        streamManager.createScope(scope);
      }
      if (!streamManager.checkStreamExists(scope, inputStream)) {
        streamManager.createStream(
            scope,
            inputStream,
            StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
      }
      if (!streamManager.checkStreamExists(scope, outputStream)) {
        streamManager.createStream(
            scope,
            outputStream,
            StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    EventStreamClientFactory clientFactory =
        EventStreamClientFactory.withScope(
            scope, ClientConfig.builder().controllerURI(controllerURI).build());
    return clientFactory.createEventWriter(
        inputStream, new UTF8StringSerializer(), EventWriterConfig.builder().build());
  }

  private static void writeEvent(EventStreamWriter<String> writer, String predictDataset)
      throws IOException {
    BufferedReader bufferedReader = new BufferedReader(new FileReader(predictDataset));
    String predictData;
    while ((predictData = bufferedReader.readLine()) != null) {
      System.out.println(predictData);
      writer
          .writeEvent(predictData)
          .whenComplete(
              (v, e) -> {
                if (e == null) {
                  System.out.println("Pravega writes event succeeded.");
                } else {
                  e.printStackTrace();
                }
              });
    }
  }

  public static void main(String[] args) throws Exception {
    writeEvent(
        createWriter(
            "tcp://localhost:9090",
            "pravega-scope",
            "predict-input-stream",
            "predict-output-stream"),
        "/Users/nicholas/Downloads/pravega_hackathon/pravega/data/predict.csv");
  }
}
